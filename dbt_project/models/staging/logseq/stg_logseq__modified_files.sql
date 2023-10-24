-- select 
--     filename,
--     split_part(filename, '.', -1) as extension,
--     -- commit_hash,
--     -- split_part(new_path, '/', 1) as path_type,
--     -- new_path,
--     -- filename,
--     -- lower(split_part(change_type, '.', 2)) as change_type,
--     --diff,
--     --diff_parsed,
--     --added_lines as added_line_count,
--     --deleted_lines as deleted_line_count,    
-- from {{ source('external_source', 'logseq__modified_files') }}
-- where split_part(filename, '.', -1) not in ('png', 'png', 'edn', 'gitignore', 'DS_Store', 'eml', 'logseq')

with a as (
    select 
        filename,
        unnest(diff_parsed['added'])[1] as line_number,
        unnest(diff_parsed['added'])[2] as line_text,
        'added' as change_type
    from {{ source('external_source', 'logseq__modified_files') }}
    where split_part(filename, '.', -1) not in ('png', 'png', 'edn', 'gitignore', 'DS_Store', 'eml', 'logseq', 'drawio')

    union

    select 
        filename,
        unnest(diff_parsed['deleted'])[1] as line_number,
        unnest(diff_parsed['deleted'])[2] as line_text,
        'deleted' as change_type
    from {{ source('external_source', 'logseq__modified_files') }}
    where split_part(filename, '.', -1) not in ('png', 'png', 'edn', 'gitignore', 'DS_Store', 'eml', 'logseq', 'drawio')
)

select
    *
from a
where line_text like '%DONE%'
