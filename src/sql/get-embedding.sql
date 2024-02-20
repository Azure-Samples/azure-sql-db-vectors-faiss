create or alter procedure dbo.get_embedding
    @text nvarchar(max),
    @embedding nvarchar(max) output
as
declare @retval int, @response nvarchar(max);
declare @payload nvarchar(max);
set @payload = json_object('input': 'isaac asimov');

exec @retval = sp_invoke_external_rest_endpoint
    @url = 'https://dm-open-ai.openai.azure.com/openai/deployments/embeddings/embeddings?api-version=2023-03-15-preview',
    @method = 'POST',
    @credential = [https://dm-open-ai.openai.azure.com],
    @payload = @payload,
    @response = @response output;

set @embedding = (
    select top(1) json_query(embedding) 
    from openjson(@response, '$.result.data[0]') with (embedding nvarchar(max) as json)
);
