create or alter function [benchmark].[vector_768$hsnw_filter](
    @metric varchar(10),
    @top int,
    @efSearch int,
    @id int,
    @v varbinary(8000)
)
returns table
as
return
-- Level 3
with [entry] as (
    select top(@efSearch)
        t.id,
        vector_distance(@metric, @v, t.vector) as distance
    from 
        (
            select distinct h.id from [$vector].faiss_hnsw h where h.l=3     
        ) l
    inner join
        [benchmark].[vector_768] t on l.id = t.id 
    order by
        distance 
),
l3 as (
    select top(@efSearch)
        t.id,
        vector_distance(@metric, @v, t.vector) as distance
    from 
        (
            select distinct id_neighbor as id 
            from [entry] pl 
            inner join [$vector].faiss_hnsw cl on cl.id = pl.id and cl.l=3
        ) l
    inner join 
        [benchmark].[vector_768] t on l.id = t.id
    order by
        distance 
),
l2 as (
    select top(@efSearch)
        t.id,
        vector_distance(@metric, @v, t.vector) as distance
    from 
        (
            select distinct id_neighbor as id 
            from l3 pl 
            inner join [$vector].faiss_hnsw cl on cl.id = pl.id and cl.l=2
        ) l
    inner join 
        [benchmark].[vector_768] t on l.id = t.id
    order by
        distance 
), 
l1 as (
    select top(@efSearch)
        t.id,
        vector_distance(@metric, @v, t.vector) as distance
    from 
        (
            select distinct id_neighbor as id 
            from [$vector].faiss_hnsw cl
            inner join l2 pl on cl.id = pl.id and cl.l=1
        ) l
    inner join 
        [benchmark].[vector_768] t on l.id = t.id
    order by
        distance 
), 
l0 as (
    select top(@efSearch)
        t.id,
        vector_distance(@metric, @v, t.vector) as distance
    from 
        (
            select distinct id_neighbor as id 
            from [$vector].faiss_hnsw cl
            inner join l1 pl on cl.id = pl.id and cl.l=0
        ) l
    inner join 
        [benchmark].[vector_768] t on l.id = t.id
    order by
        distance 
)
select top(@top)
    t.id,
    vector_distance(@metric, @v, t.vector) as distance
from 
    (
        select distinct id_neighbor as id 
        from [$vector].faiss_hnsw cl
        inner join l0 pl on cl.id = pl.id and cl.l=0
    ) f
inner join 
    [benchmark].[vector_768] t on f.id = t.id
where 
    t.id >= @id
order by
    distance 

