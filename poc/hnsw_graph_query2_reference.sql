declare @r nvarchar(max) = 
''

select t.id, title, r.distance from dbo.wikipedia_articles_title_embeddings_native t
inner join
(
select cast([key] as int) as id, cast([value] as float) as distance from openjson(@r)
) r on t.id = r.id
order by distance