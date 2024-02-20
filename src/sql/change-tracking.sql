if not exists(select * from sys.change_tracking_databases where database_id = db_id())
begin
    alter database vectordb
    set change_tracking = on
    (change_retention = 30 days, auto_cleanup = on)
end
go

if not exists(select * from sys.change_tracking_tables where [object_id]=object_id('dbo.wikipedia_articles_embeddings'))
begin
    alter table dbo.wikipedia_articles_embeddings
    enable change_tracking
end
go

select * from sys.change_tracking_tables


select * from dbo.wikipedia_articles_embeddings where title = 'Isaac Asimov'

delete from dbo.wikipedia_articles_embeddings where id = 1008698

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