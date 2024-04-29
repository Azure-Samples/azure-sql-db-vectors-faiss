drop table if exists #t3
drop table if exists #t2
drop table if exists #t1
drop table if exists #t0
;

declare @v varbinary(8000)
declare @t int = 10
declare @efSearch int = 32 -- probes
select @v = title_vector_native from [dbo].[wikipedia_articles_title_embeddings_native] where title = 'Seattle'
;

-- Level 3
select top(@efSearch)
    t.id,
    vector_distance('cosine', @v, t.title_vector_native) as cosine_distance
into 
    #t3
from 
    [dbo].[wikipedia_articles_title_embeddings_native] t 
inner join
    [$vector].faiss_hnsw l on l.id = t.id and l.l=3
order by
    cosine_distance 
;

-- Level 2
with cte as (
    select distinct id_neighbor as id 
    from [$vector].faiss_hnsw l
    inner join #t3 pl on l.id = pl.id and l.l=2
)
select top(@efSearch)
    t.id,
    vector_distance('cosine', @v, t.title_vector_native) as cosine_distance
into 
    #t2
from 
    cte l
inner join 
    [dbo].[wikipedia_articles_title_embeddings_native] t on l.id = t.id
order by
    cosine_distance 
;

-- Level 1
with cte as (
    select distinct id_neighbor as id 
    from [$vector].faiss_hnsw l
    inner join #t2 pl on l.id = pl.id and l.l=1
)
select top(@efSearch)
    t.id,
    vector_distance('cosine', @v, t.title_vector_native) as cosine_distance
into 
    #t1
from 
    cte l
inner join 
    [dbo].[wikipedia_articles_title_embeddings_native] t on l.id = t.id
order by
    cosine_distance 
;

-- Level 0
with cte as (
    select distinct id_neighbor as id 
    from [$vector].faiss_hnsw l
    inner join #t1 pl on l.id = pl.id and l.l=0
)
select top(@efSearch)
    t.id,
    vector_distance('cosine', @v, t.title_vector_native) as cosine_distance
into 
    #t0
from 
    cte l
inner join 
    [dbo].[wikipedia_articles_title_embeddings_native] t on l.id = t.id
order by
    cosine_distance 
;

-- Find the top 10 closest articles
with cte as (
    select distinct id_neighbor as id 
    from [$vector].faiss_hnsw l
    inner join #t0 pl on l.id = pl.id and l.l=0
)
select top(@t)
    t.id,
    vector_distance('cosine', @v, t.title_vector_native) as cosine_distance
from 
    cte f
inner join 
    [dbo].[wikipedia_articles_title_embeddings_native] t on f.id = t.id
order by
    cosine_distance 

