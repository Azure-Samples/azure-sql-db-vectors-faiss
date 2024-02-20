
select * from dbo.wikipedia_articles_embeddings where title = 'Isaac Asimov'
go

insert into dbo.wikipedia_articles_embeddings
select 
    id + 1000000 as id,
    url,
    title,
    text,
    title_vector,
    content_vector,
    vector_id + 1000000000 as vector_id 
from 
    dbo.wikipedia_articles_embeddings where title = 'Isaac Asimov'
go

drop table if exists #t;
declare @e nvarchar(max) = (select top 1 content_vector from dbo.wikipedia_articles_embeddings where id = 1008698)
select cast([value] as float) * 0.2 as v into #t from openjson(@e)
declare @e2 nvarchar(max) = (select '[' + string_agg(cast([v] as nvarchar(max)), ',') + ']' from #t)
update dbo.wikipedia_articles_embeddings set content_vector = @e2 where id = 1008698 
go

delete from dbo.wikipedia_articles_embeddings where id = 1008698
go




