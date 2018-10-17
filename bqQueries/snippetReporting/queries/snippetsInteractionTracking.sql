-- Query to pull snippet tracking information to match GA https://analytics.google.com/analytics/web/#/report/content-event-events/a36116321w119701667p134921338/_u.date00=20181014&_u.date01=20181014&_r.drilldown=analytics.eventCategory:impression&explorer-table.plotKeys=%5B%5D/
-- This is the base query of the snippet reporting
-- Tells us which snippets were viewed, how many were clicked and how many blocked the snippet

WITH data AS(
  SELECT
    visitData.date,
    visitData.snippetID,
    visitData.country,
    visitData.eventCategory,
    -- Get statistics for top 10 events. All other = other
    CASE WHEN eventCategory = 'impression' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS impression,
    CASE WHEN eventCategory = 'snippet-blocked' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS snippetBlocked,
    CASE WHEN eventCategory = 'click' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS click,
    CASE WHEN eventCategory = 'conversion-mobile-activation-email' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS convMobileActEmail,
    CASE WHEN eventCategory = 'signup-success' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS signupSuccess,
    CASE WHEN eventCategory = 'button-click' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS buttonClick,
    CASE WHEN eventCategory = 'snippet-scene2-blocked' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS snipScene2Blocked,
    CASE WHEN eventCategory = 'conversion-mobile-activation-sms' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS convMobileActSMS,
    CASE WHEN eventCategory = 'signup-error' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS signupError,
    CASE WHEN eventCategory = 'conversion-subscribe-activation' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS convSubscribeActiv,
    CASE WHEN eventCategory NOT IN('impression','snippet-blocked', 'click', 'conversion-mobile-activation-email', 'signup-success','button-click','snippet-scene2-blocked', 'conversion-mobile-activation-sms', 'signup-error', 'conversion-subscribe-activation') THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS other
  FROM (
    SELECT
      date,
      geoNetwork.country,
      fullVisitorId,
      eventInfo.eventAction AS snippetID,
      eventInfo.eventCategory
    FROM
      `ga-mozilla-org-prod-001.125230768.ga_sessions_20181014`,
      UNNEST (hits) AS hits
    GROUP BY 1,2,3,4,5) AS visitData
  GROUP BY
    1,2,3,4
  ORDER BY 4 DESC)

-- Aggregate by date, snippetID and country
SELECT
  date,
  snippetID,
  country,
  SUM(impression) AS impression,
  SUM(snippetBlocked) AS snippetBlocked,
  SUM(click) AS click,
  SUM(convMobileActEmail) AS convMobileActEmail,
  SUM(signupSuccess) as signupSuccess,
  SUM(buttonClick) as buttonClick,
  SUM(snipScene2Blocked) as snipScene2Blocked,
  SUM(convMobileActSMS) as convMobileActSMS,
  SUM(signupError) as signupError,
  SUM(convSubscribeActiv) as convSubscribeActiv,
  SUM(other) as other
FROM
  data
GROUP BY 1,2,3
ORDER BY 4 DESC

