select
    hash,
    author_date,
    committer_date,
    deletions,
    insertions,
    lines,
    files
from {{ source('external_source', 'logseq__commits') }}