## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features. We recognize it's a bit unclean to define these in multiple places, but at this point it's the only workaround if you'd like your custom conn type to show up in the Airflow UI.
def get_provider_info():
    return {
        "package-name": "airflow-provider-lakefs",
        "name": "lakeFS Airflow Provider",
        "description": "An Airflow provider of lakeFS",
        "hook-class-names": ["lakefs_provider.hooks.lakefs_hook.LakeFSHook"],
        "connection-types": [{
            'connection-type': 'lakefs',
            'hook-class-name': 'lakefs_provider.hooks.lakefs_hook.LakeFSHook',
        }],
        "extra-links": ["lakefs_provider.operators.create_branch_operator.ExtraLink"],
        "versions": ["0.0.1"]
    }
