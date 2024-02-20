-- Find all the articles similar to a defined text, using the FAISS index
-- Please note that if the container is sleeping, the first call will take a second to respond

-- Get the embedding
-- declare @v nvarchar(max)
-- select @v = json_object('vector': json_query(content_vector)) from dbo.wikipedia_articles_embeddings where title = 'isaac asimov'
declare @v nvarchar(max)
exec dbo.get_embedding 'isaac asimov', @v output
select @v

-- Save embeeding to avoid OpenAI abuse :)
drop table if exists #v
select * into #v from (values(@v)) as t(embedding)
go

-- Query the index
declare @v nvarchar(max) = (select top(1) embedding from #v)
declare @payload nvarchar(max) = json_object('vector': json_query(@v));
declare @retval int, @response nvarchar(max);
exec @retval = sp_invoke_external_rest_endpoint
    @url = 'https://dm-faiss3.kindglacier-14626d21.centralus.azurecontainerapps.io/index/faiss/query',
    @method = 'POST',
    @payload = @payload,
    @response = @response output;

select @response;

with cte as
(
    select [key] as article_id, 1-cast([value] as float) as cosine_distance 
    from openjson(@response, '$.result.result')
)
select c.*, a.title from cte c 
left join dbo.wikipedia_articles_embeddings a on a.id = c.article_id
order by cosine_distance desc
go

/*
-- New vector to insert, associated to the id 123456789
declare @v nvarchar(max)
select @v = json_object(
        'id': 123456789, 
        'vector': json_query(content_vector)
) from dbo.wikipedia_articles_embeddings where title = 'isaac asimov'
select @v

-- Update the index
declare @payload nvarchar(max) = json_object('input': 'isaac asimov');
declare @retval int, @response nvarchar(max);
exec @retval = sp_invoke_external_rest_endpoint
    @url = 'https://dm-faiss.purpleflower-af782b2e.centralus.azurecontainerapps.io/index/faiss/add',
    @method = 'POST',
    @payload = @v,
    @response = @response output;
select @response
*/