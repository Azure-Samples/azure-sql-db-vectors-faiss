set statistics time on
declare @v varbinary(8000)
select @v = vector from [benchmark].[vector_768] where id = 100; 

select top(10)
    id,
    vector_distance('cosine', @v, t.vector) as cosine_distance 
from
    [benchmark].[vector_768] t
order by
    cosine_distance
option
    (maxdop 1)
