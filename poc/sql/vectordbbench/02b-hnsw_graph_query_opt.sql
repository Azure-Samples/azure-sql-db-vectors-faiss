set statistics time on

declare @top int = 10
declare @efSearch int = 32
declare @v varbinary(8000)
select @v = vector from [benchmark].[vector_768] where id=100;

-- Level 3
with l3 as (
    select top(@efSearch)
        t.id,
        vector_distance('cosine', @v, t.vector) as cosine_distance
    from 
        [benchmark].[vector_768] t 
    inner join
        [$vector].faiss_hnsw l on l.id = t.id and l.l=3
    order by
        cosine_distance 
), 
l2 as (
    select top(@efSearch)
        t.id,
        vector_distance('cosine', @v, t.vector) as cosine_distance
    from 
        (
            select distinct id_neighbor as id 
            from [$vector].faiss_hnsw cl
            inner join l3 pl on cl.id = pl.id and cl.l=2
        ) l
    inner join 
        [benchmark].[vector_768] t on l.id = t.id
    order by
        cosine_distance 
), 
l1 as (
    select top(@efSearch)
        t.id,
        vector_distance('cosine', @v, t.vector) as cosine_distance
    from 
        (
            select distinct id_neighbor as id 
            from [$vector].faiss_hnsw cl
            inner join l2 pl on cl.id = pl.id and cl.l=1
        ) l
    inner join 
        [benchmark].[vector_768] t on l.id = t.id
    order by
        cosine_distance 
), 
l0 as (
    select top(@efSearch)
        t.id,
        vector_distance('cosine', @v, t.vector) as cosine_distance
    from 
        (
            select distinct id_neighbor as id 
            from [$vector].faiss_hnsw cl
            inner join l1 pl on cl.id = pl.id and cl.l=0
        ) l
    inner join 
        [benchmark].[vector_768] t on l.id = t.id
    order by
        cosine_distance 
)
select top(@top)
    t.id,
    vector_distance('cosine', @v, t.vector) as cosine_distance
from 
    (
        select distinct id_neighbor as id 
        from [$vector].faiss_hnsw cl
        inner join l0 pl on cl.id = pl.id and cl.l=0
    ) f
inner join 
    [benchmark].[vector_768] t on f.id = t.id
order by
    cosine_distance 
option
    (maxdop 1)
