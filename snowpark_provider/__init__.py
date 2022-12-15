## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features. We recognize it's a bit unclean to define these in multiple places, but at this point it's the only workaround if you'd like your custom conn type to show up in the Airflow UI.
def get_provider_info():
    return {
        "package-name": "airflow-provider-snowpark",  # Required
        "name": "Snowpark Airflow Provider",  # Required
        "description": "A snowpark decorator provider.",  # Required
        "hook-class-names": ["snowpark_provider.hooks.snowpark_hook.SnowparkHook"],
        "extra-links": [
            "snowpark_provider.decorators.dataframe_decorator.dataframe_decorator"
        ],
        "versions": ["0.0.1"],  # Required
    }
