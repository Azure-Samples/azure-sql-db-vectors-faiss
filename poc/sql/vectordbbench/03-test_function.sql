set statistics time on
declare @v varbinary(8000)
select @v = vector from [benchmark].[vector_768] where id = 100; 

select 
    *
from
    [benchmark].[vector_768$hsnw_search]('cosine', 10, 48, @v)
order by
    distance
