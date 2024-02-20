
-- 'isaac asimov
declare @r nvarchar(max) = N'
{"8698": 0.09996724128723145, "60390": 0.1554587483406067, "55040": 0.16136479377746582, "15252": 0.17107820510864258, "5123": 0.1762905716896057, "4496": 0.17816877365112305, "23889": 0.1835460662841797, "7277": 0.19082367420196533, "67314": 0.19412505626678467, "61086": 0.19422698020935059}
'
;with cte as
(
    select  
        cast([key] as int) as [id],
        cast([value] as float) as [distance]
    from
        openjson(@r) r
)
select 
    a.id,
    a.title,
    r.distance,
    1-r.distance as [similarity]
from
    cte r
inner join
    dbo.wikipedia_articles_embeddings a on r.id = a.id
order by
    [distance]