SELECT COUNT(*) AS instances,
       DAY,
       CASE
           WHEN VERSION = '0' THEN 'Failed installation'
           WHEN VERSION IS NULL
                OR version_major = 0 THEN 'version missing'
           WHEN old_version='0' THEN 'Fresh new install'
           WHEN old_version IS NULL
                OR old_version_major = 0 THEN 'old_version missing'
           WHEN old_version=VERSION THEN 'Installed same version as current'
           WHEN old_version_major>version_major
                OR (old_version_major=version_major
                    AND (old_version_minor>version_minor
                         OR (old_version_minor=version_minor
                             AND old_version_micro>version_micro))) THEN 'Installed older version than current'
           WHEN old_version_major<version_major
                OR (old_version_major=version_major
                    AND (old_version_minor<version_minor
                         OR (old_version_minor=version_minor
                             AND old_version_micro<version_micro))) THEN 'Installed more recent version than current'
           ELSE 'Other'
       END AS reinstalled,
       attribution
FROM
  (SELECT DAY,
          VERSION,
          old_version,
          attribution,
          SPLIT_PART(clean_version, '.', 1)::INT AS version_major,
          SPLIT_PART(clean_version, '.', 2)::INT AS version_minor,
          SPLIT_PART(clean_version, '.', 3)::INT AS version_micro,
          SPLIT_PART(clean_old_version, '.', 1)::INT AS old_version_major,
          SPLIT_PART(clean_old_version, '.', 2)::INT AS old_version_minor,
          SPLIT_PART(clean_old_version, '.', 3)::INT AS old_version_micro
   FROM
     (SELECT TRUNC(TIMESTAMP) AS DAY,
             VERSION,
             old_version,
             attribution,
             CASE
                 WHEN VERSION ~ '^\\d+\\.\\d+\\.\\d+$' THEN VERSION
                 WHEN VERSION ~ '^\\d+\\.\\d+$' THEN VERSION||'.0'
                 ELSE '0.0.0'
             END AS clean_version,
             CASE
                 WHEN old_version ~ '^\\d+\\.\\d+\\.\\d+$' THEN old_version
                 WHEN old_version ~ '^\\d+\\.\\d+$' THEN old_version||'.0'
                 ELSE '0.0.0'
             END AS clean_old_version
      FROM download_stats_year
      WHERE build_channel = 'release'))
WHERE reinstalled IN ('Installed more recent version than current', 'Installed older version than current', 'Installed same version as current', 'version missing')
GROUP BY reinstalled,
         DAY, attribution