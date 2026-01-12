--show what a role has access to.
SHOW GRANTS TO ROLE personal_role__raquelhipolito;
SHOW GRANTS TO ROLE personal_role__warsanabdullahi;

SELECT *
FROM raw_vault_mvp.snowflake_uac.user_roles
WHERE loaded_at = (
    SELECT MAX(loaded_at)
    FROM raw_vault_mvp.snowflake_uac.user_roles
)

GRANT USAGE ON SCHEMA show grants TO ROLE TO ROLE personal_role__robinpatel;

GRANT SELECT ON TABLE se.data.se_user_attributes TO ROLE personal_role__robinpatel;


