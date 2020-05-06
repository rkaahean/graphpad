### dbt project for Insightful Science Data Warehouse

### Using the starter project

Try running the following commands:

- dbt run
- dbt test

### Notes

views created by dbt will have no grants. Will need to grant permissions on the views (after every build)

```
GRANT SELECT ON ALL VIEWS IN SCHEMA "GRAPHPAD_DB"."DBT_PHIL" TO ROLE PERISCOPE_ROLE;
```

https://support.snowflake.net/s/article/faq-why-cant-roles-access-new-tables-in-a-schema

Ralph: Not sure if this helps: https://docs.getdbt.com/docs/building-a-dbt-project/building-models/snowflake-configs#copying-grants

### Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/overview)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
