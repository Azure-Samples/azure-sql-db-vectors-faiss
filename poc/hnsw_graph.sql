create table [$vector].[faiss_hnsw]
(
	[id] [int] not null, -- vector id
	[id_neighbor] [int] not null, -- neighbors
	[l] [int] not null -- level
) on [primary]
go

create clustered index ixc on [$vector].[faiss_hnsw] (l, id) with (data_compression = page, drop_existing = on);
go

select used_page_count * 8. / 1024., row_count from sys.dm_db_partition_stats
where object_id = object_id('$vector.faiss_hnsw')
go

select l, count(*) from [$vector].faiss_hnsw with (readuncommitted) group by l order by l
go