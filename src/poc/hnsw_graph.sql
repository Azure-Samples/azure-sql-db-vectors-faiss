create table [$vector].[faiss_hnsw]
(
	[id] [int] not null,
	[id_neighbor] [int] not null,
	[l] [int] not null
) on [primary]
go

create clustered index ixc on [$vector].[faiss_hnsw] (l, id) with (data_compression = page, drop_existing = on);
go

select used_page_count * 8. / 1024., row_count from sys.dm_db_partition_stats
where object_id = object_id('$vector.faiss_hnsw')
go