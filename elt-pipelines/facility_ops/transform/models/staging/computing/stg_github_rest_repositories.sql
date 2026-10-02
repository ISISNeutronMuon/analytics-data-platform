with source as (
    select * from {{source('computing_github_rest', 'repositories')}}
),

cleaned as (
    select
        name,
        owner,
        public,
        fork,
        default_branch,
        repo_size_kilobytes,
        license,
        readme_size_kilobytes
)

select * from cleaned
