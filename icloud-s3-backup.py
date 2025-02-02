import datetime
import os
import tarfile
from datetime import timedelta, timezone
from functools import wraps

import boto3
import click
from botocore.exceptions import ProfileNotFound

# Maximum retention days for backups
MAX_RETENTION_DAYS = 90

def handle_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            click.secho(f"🚨 Error: {str(e)}", fg='red')
            raise click.Abort()

    return wrapper


@click.command()
@click.argument('profile', metavar='<AWS_PROFILE>')
@click.argument('bucket', metavar='<S3_BUCKET>')
@click.argument('folder',
                type=click.Path(exists=True, file_okay=False, dir_okay=True),
                metavar='<LOCAL_FOLDER>')
@handle_errors
def main(profile, bucket, folder):
    s3_client = initialize_aws_session(profile)
    delete_old_backups(s3_client, bucket)
    archive_path = create_backup_archive(folder)
    upload_to_s3(s3_client, bucket, archive_path)
    print_summary(folder, bucket, archive_path)
    clear_tmp_files(archive_path)


def initialize_aws_session(profile):
    """Initialize AWS session with given profile"""
    try:
        session = boto3.Session(profile_name=profile)
        s3 = session.client('s3')
        click.secho("✓ AWS session initialized", fg='green')
        return s3
    except ProfileNotFound:
        raise Exception(f"AWS profile '{profile}' not found")


def delete_old_backups(s3_client, bucket):
    """Delete backups older than 90 days"""
    click.secho(f"\n🕵️  Scanning for backups folder than {MAX_RETENTION_DAYS} days...", fg='yellow')
    cutoff = datetime.datetime.now(timezone.utc) - timedelta(days=MAX_RETENTION_DAYS)
    to_delete = []

    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket):
        if 'Contents' in page:
            to_delete.extend([
                obj['Key'] for obj in page['Contents']
                if obj['LastModified'] < cutoff
            ])

    if not to_delete:
        click.secho("✓ No old backups found for cleanup", fg='green')
        return

    click.secho(f"🗑️  Found {len(to_delete)} items to delete", fg='yellow')
    with click.progressbar(to_delete, label='Deleting old backups') as items:
        for key in items:
            s3_client.delete_object(Bucket=bucket, Key=key)
    click.secho("✓ Old backups cleaned successfully", fg='green')


def create_backup_archive(folder_path):
    """Create compressed tar archive of folder"""
    click.secho("\n📦 Creating backup archive...", fg='yellow')
    timestamp = datetime.datetime.now().strftime("%B-%Y")
    archive_name = f"{timestamp}.tar"

    total_files = sum(len(files) for _, _, files in os.walk(folder_path))

    with tarfile.open(archive_name, "w") as tar:
        with click.progressbar(length=total_files, label='Adding files') as pbar:
            for root, _, files in os.walk(folder_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    arcname = os.path.relpath(full_path, start=folder_path)
                    tar.add(full_path, arcname=arcname)
                    pbar.update(1)

    click.secho(f"✓ Archive created: {archive_name}", fg='green')
    return archive_name


def upload_to_s3(s3_client, bucket, file_path):
    """Upload file to S3 with progress tracking"""
    click.secho("\n☁️  Uploading to S3...", fg='yellow')
    file_size = os.path.getsize(file_path)

    with click.progressbar(length=file_size, label='Upload progress') as pbar:
        def update_progress(chunk):
            pbar.update(chunk)

        s3_client.upload_file(
            Filename=file_path,
            Bucket=bucket,
            Key=os.path.basename(file_path),
            Callback=update_progress
        )
    click.secho("✓ Upload completed successfully", fg='green')


def clear_tmp_files(*files):
    """Delete temporary files after backup"""
    click.secho("\n🧹 Cleaning up temporary files...", fg='yellow')
    for file in files:
        os.remove(file)
    click.secho("✓ Temporary files cleaned up", fg='green')


def print_summary(folder_path, bucket, archive_path):
    """Display final backup summary"""
    file_size = os.path.getsize(archive_path) / 1024 / 1024
    click.secho("\n📊 Backup Summary", fg='cyan', bold=True)
    click.echo(f"📂 Source Folder: {folder_path}")
    click.echo(f"🗜️  Archive Size: {file_size:.2f} MB")
    click.echo(f"🏷️  S3 Location: s3://{bucket}/{os.path.basename(archive_path)}")


if __name__ == "__main__":
    main()
