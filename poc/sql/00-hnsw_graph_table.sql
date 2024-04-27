create schema [$vector] authorization [dbo]
go

drop table if exists [$vector].[faiss_hnsw]
create table [$vector].[faiss_hnsw]
(
	[id] [int] not null, -- vector id
	[id_neighbor] [int] not null, -- neighbors
	[l] [int] not null -- level
) 
go

create clustered index ixc on [$vector].[faiss_hnsw] (l, id) 
with (data_compression = page, drop_existing = off);
go

