from importlib.resources import path
import pandas as pd
import requests
import workalendar.europe as workalendar
from .generic_tasks import write_raw, read_watermark
from prefect import flow, task
import os
from pydriller import Repository
from dotenv import load_dotenv
import datetime
from datetime import datetime, timedelta

load_dotenv()

urls = ["https://github.com/bayees/logseq.git"]

def extract_modified_files(hash, files) -> pd.DataFrame:
    
    modified_files_df = pd.DataFrame()

    for file in files:
            file_dict = {
                "commit_hash": hash,
                "old_path": file.old_path,
                "new_path": file.new_path,
                "filename": file.filename,
                "change_type": file.change_type,
                "diff": file.diff,
                "diff_parsed": file.diff_parsed,
                "added_lines": file.added_lines,
                "deleted_lines": file.deleted_lines,
                "source_code": file.source_code,
                "source_code_before": file.source_code_before,
                "methods": file.methods,
                "methods_before": file.methods_before,
                "changed_methods": file.changed_methods,
                "nloc": file.nloc,
                "complexity": file.complexity,
                "token_count": file.token_count,
            }

            modified_files_df = pd.concat([modified_files_df, pd.DataFrame([file_dict])], ignore_index=True)

    return modified_files_df

@task
def extract_commits(watermark: datetime) -> str:
    if watermark:
        watermark + timedelta(seconds=1)
    repo = Repository(path_to_repo=urls, since=watermark).traverse_commits()

    # Initialize empty dataframes to store commit and modified file information
    commits_df = pd.DataFrame()
    modified_files_df = pd.DataFrame()

    # Extract commit and modified file information and store it in the dataframes
    for commit in repo:
        commit_dict = {
            "hash": commit.hash,
            "author_email": commit.author.email,
            "author_name": commit.author.name,
            "committer_email": commit.committer.email,
            "committer_name": commit.committer.name,
            "author_date": commit.author_date,
            "author_timezone": commit.author_timezone,
            "committer_date": commit.committer_date,
            "committer_timezone": commit.committer_timezone,
            "branches": commit.branches,
            "in_main_branch": commit.in_main_branch,
            "merge": commit.merge,
            "parents": commit.parents,
            "project_name": commit.project_name,
            "project_path": commit.project_path,
            "deletions": commit.deletions,
            "insertions": commit.insertions,
            "lines": commit.lines,
            "files": commit.files,
            "dmm_unit_size": commit.dmm_unit_size,
            "dmm_unit_complexity": commit.dmm_unit_complexity,
            "dmm_unit_interfacing": commit.dmm_unit_interfacing,
        }

        commits_df = pd.concat([commits_df, pd.DataFrame([commit_dict])], ignore_index=True)

        modified_files_df =  pd.concat([modified_files_df, extract_modified_files(commit.hash, commit.modified_files)], ignore_index=True)

    return commits_df, modified_files_df


@flow
def extract__logseq():
    watermark = read_watermark('author_date', 'raw/logseq__commits/*')
    commits_extract, modified_files_extract = extract_commits(watermark)
    write_raw(commits_extract, f'logseq__commits/logseq__commits_increment__{int(datetime.now().strftime("%Y%m%d"))}')
    write_raw(modified_files_extract, f'logseq__modified_files/logseq__modified_files_increment__{int(datetime.now().strftime("%Y%m%d"))}')

if __name__ == "__main__":
    extract__logseq()
