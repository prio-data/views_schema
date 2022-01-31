
from typing import Optional, List
import datetime
import pydantic

class ModelMetadata(pydantic.BaseModel):
    """
    ModelMetadata
    =============

    Data used to organize model objects.

    parameters:
        author (str): Name of the user that authored the model object.
        queryset_name (str): Name of the queryset used to train the model
        train_start (int): Month identifier for training start date
        train_start (int): Month identifier for training end date
        training_date (datetime.datetime): Timestamp for training date (use datetime.datetime.now())

    example:

        # Instantiate the class with values

        my_metadata = ModelMetadata(
            author = "my_name",
            queryset_name = "my_queryset",
            train_start = 1,
            train_end = 300,
            steps = [1,2,3],
            training_date = datetime.datetime.now())

        # Create metadata with a views_runs.ViewsRun object. This fetches
        # values from the associated StepshiftedModels and DataPartitioner
        # objects.

        my_metadata = my_run.create_model_metadata(
                author = "me",
                queryset_name = "my_queryset",
                training_partition_name = "A",
                )

    """

    author:        str

    queryset_name: str

    train_start:   int
    train_end:     int

    steps:         Optional[List[int]] = None

    training_date: datetime.datetime
