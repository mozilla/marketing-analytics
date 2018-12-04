SELECT
*,
snippetMetaData.name as snippetName,
snippetMetaData.campaign as snippetCampaign

FROM `snippets.testDataSource` as performance

LEFT JOIN
`snippets.snippetMetaData` as snippetMetaData

ON performance.snippetID = snippetMetaData.ID