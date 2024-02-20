select 
    id, [type], item_count, vector_dimensions, [version], 
    datalength([data]) / 1024. / 1024 as size_mb 
from 
    [$vector].[index]

--delete from [$vector].[index] where id = 1

