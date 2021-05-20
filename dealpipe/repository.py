from dagster import repository

from dealpipe.pipelines.process_deals import process_deals


@repository
def dealpipe():
    """
    The repository definition for this dealpipe Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    pipelines = [process_deals]
    schedules = []
    sensors = []

    return pipelines + schedules + sensors
