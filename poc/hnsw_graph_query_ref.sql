set statistics time on

declare @v varbinary(8000)
select @v = title_vector_native from [dbo].[wikipedia_articles_title_embeddings_native] where title = 'Seattle';

select top(10)
    id,
    title,
    vector_distance('cosine', @v, t.title_vector_native) as cosine_distance 
from
    dbo.wikipedia_articles_title_embeddings_native t
order by
    cosine_distance