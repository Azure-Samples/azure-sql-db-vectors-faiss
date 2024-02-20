/*
    Create user
*/
if (user_id('vector_user') is null) begin
    create user [vector_user] with password = 'kOZ0I9DZ_mu4JXyETWH@2VQ8ovNQYOcmriJYEh9o='
    alter role db_owner add member [vector_user];
end
go

/*
	Cleanup if needed
*/
if not exists(select * from sys.[external_data_sources] where name = 'vector_database_wikipedia_articles_embedded')
begin
	create external data source [vector_database_wikipedia_articles_embedded]
	with 
	( 
		type = blob_storage,
		location = 'https://dmstore3.blob.core.windows.net/playground/wikipedia'
	);
	end
go

/*
	Create table
*/
drop table if exists [dbo].[wikipedia_articles_embeddings];
create table [dbo].[wikipedia_articles_embeddings]
(
	[id] [int] not null,
	[url] [varchar](1000) not null,
	[title] [varchar](1000) not null,
	[text] [varchar](max) not null,
	[title_vector] [varchar](max) not null,
	[content_vector] [varchar](max) not null,
	[vector_id] [int] not null
)
go

/*
	Import data
*/
bulk insert dbo.[wikipedia_articles_embeddings]
from 'vector_database_wikipedia_articles_embedded.csv'
with (
	data_source = 'vector_database_wikipedia_articles_embedded',
    format = 'csv',
    firstrow = 2,
    codepage = '65001',
	fieldterminator = ',',
	rowterminator = '0x0a',
    fieldquote = '"',
    batchsize = 1000,
    tablock
)
go

/*
	Add primary key
*/
alter table [dbo].[wikipedia_articles_embeddings]
add constraint pk__wikipedia_articles_embeddings primary key nonclustered (id)
go

/*
	Verify data
*/
select top (100) * from [dbo].[wikipedia_articles_embeddings]
go
select * from [dbo].[wikipedia_articles_embeddings] where title = 'Alan Turing'
go





