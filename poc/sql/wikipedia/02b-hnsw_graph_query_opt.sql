set statistics time on

declare @top int = 10
declare @efSearch int = 32
declare @v varbinary(8000)
select @v = title_vector_native from [dbo].[wikipedia_articles_title_embeddings_native] where title = 'Seattle';

-- Level 3
with l3 as (
    select top(@efSearch)
        t.id,
        vector_distance('cosine', @v, t.title_vector_native) as cosine_distance
    from 
        [dbo].[wikipedia_articles_title_embeddings_native] t 
    inner join
        [$vector].faiss_hnsw l on l.id = t.id and l.l=3
    order by
        cosine_distance 
), 
l2 as (
    select top(@efSearch)
        t.id,
        vector_distance('cosine', @v, t.title_vector_native) as cosine_distance
    from 
        (
            select distinct id_neighbor as id 
            from [$vector].faiss_hnsw cl
            inner join l3 pl on cl.id = pl.id and cl.l=2
        ) l
    inner join 
        [dbo].[wikipedia_articles_title_embeddings_native] t on l.id = t.id
    order by
        cosine_distance 
), 
l1 as (
    select top(@efSearch)
        t.id,
        vector_distance('cosine', @v, t.title_vector_native) as cosine_distance
    from 
        (
            select distinct id_neighbor as id 
            from [$vector].faiss_hnsw cl
            inner join l2 pl on cl.id = pl.id and cl.l=1
        ) l
    inner join 
        [dbo].[wikipedia_articles_title_embeddings_native] t on l.id = t.id
    order by
        cosine_distance 
), 
l0 as (
    select top(@efSearch)
        t.id,
        vector_distance('cosine', @v, t.title_vector_native) as cosine_distance
    from 
        (
            select distinct id_neighbor as id 
            from [$vector].faiss_hnsw cl
            inner join l1 pl on cl.id = pl.id and cl.l=0
        ) l
    inner join 
        [dbo].[wikipedia_articles_title_embeddings_native] t on l.id = t.id
    order by
        cosine_distance 
)
select top(@top)
    t.id,
    t.title,
    vector_distance('cosine', @v, t.title_vector_native) as cosine_distance
from 
    (
        select distinct id_neighbor as id 
        from [$vector].faiss_hnsw cl
        inner join l0 pl on cl.id = pl.id and cl.l=0
    ) f
inner join 
    [dbo].[wikipedia_articles_title_embeddings_native] t on f.id = t.id
order by
    cosine_distance 

