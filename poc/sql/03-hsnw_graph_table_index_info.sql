select used_page_count * 8. / 1024. as size_in_mb, row_count from sys.dm_db_partition_stats
where object_id = object_id('$vector.faiss_hnsw')
go

select l, count(*) from [$vector].faiss_hnsw with (readuncommitted) group by l order by l
go