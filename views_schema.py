"""
The classes in this module define the (json) schema which services and tools
use to communicate.
"""
import datetime
from typing import List,Optional,Dict,Any,TypeVar, Tuple
import pydantic

T = TypeVar("T")

# =QUERYSET MANAGER MODELS================================
# These models are used by the queryset_manager service,
# and the viewser CLI when communicating about querysets.

class Operation(pydantic.BaseModel):
    """
    A path-element in a path defining a data column.
    May be a DatabaseOperation or a TransformOperation.
    """
    class Config:
        orm_mode = True

    namespace: str
    name: str
    arguments: List[str]

class DatabaseOperation(Operation):
    """
    The terminal operation of a path defining a data column.
    The name attribute points to a table.column in the database.
    The arguments attribute is either "values", or in the case of
    aggregation, the name of an aggregation function.
    """
    class Config:
        orm_mode = True

    namespace = "base"
    arguments: List[str] = ["values"]

class TransformOperation(Operation):
    """
    A non-terminal operation in a path defining a data column.  The name
    attribute points to a module.function in the transform service, which is
    applied to the subsequent data in the path.
    """
    class Config:
        orm_mode = True

    namespace = "trf"

class RenameOperation(TransformOperation):
    name = "util.rename"

class ListedQueryset(pydantic.BaseModel):
    name: str
    loa: str
    themes: List[str] = []
    description: Optional[str] = None

class PostedQueryset(ListedQueryset):
    operations: List[List[Operation]]

class DetailQueryset(PostedQueryset):
    pass

class Queryset(DetailQueryset):
    pass

# =DOCS MODELS============================================
# These models are accepted and returned by the views_docs
# service, and are returned by various services that
# expose documentation through the service.

class DocumentationEntry(pydantic.BaseModel):
    """
    Services that expose endpoints that return documentation entries
    can be documented through the views_docs service.

    Note that the entry is recursive: entries can refer to child
    entries, such as tables > columns.
    """
    name: str
    path: str = "."
    entries: List["DocumentationEntry"] = []
    data: Dict[str,Any] = {}

DocumentationEntry.update_forward_refs()

class PostedDocumentationPage(pydantic.BaseModel):
    """
    The schema for posting a documentationentry to
    the views_docs service.
    """
    content: str

class DocumentationPageListEntry(pydantic.BaseModel):
    """
    The schema used when listing documentationentries.
    """
    name: str
    category: str
    last_edited: datetime.datetime
    author: str = ""

DocumentationPageList = List[DocumentationPageListEntry]

class DocumentationPageDetail(DocumentationPageListEntry, PostedDocumentationPage):
    """
    The schema for showing a full documentation entry, with both content and metadata
    """

class ViewsDoc(pydantic.BaseModel):
    """
    The data returned by views_docs, combining a remote entry with
    an optional documentation page.
    """
    entry: DocumentationEntry
    page: Optional[DocumentationPageDetail] = None

# =PARTITIONING MODELS====================================
# These models are related to time-partitioning. In ViEWS,
# time-partitions are defined as nested dictionaries.

PartitionDictionaries = Dict[str, Dict[str, T]]
PartitionTimes = PartitionDictionaries[Tuple[int,int]]
