from typing import Union, Optional, Set

from streamlod.entities.identifiable_entity import CulturalHeritageObject
from streamlod.utils import rank

class Activity:
    def __init__(
        self,
        refersTo: CulturalHeritageObject,
        institute: str,
        person: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        tool: Optional[Set[str]] = None
    ):
        self.institute = institute
        self.person = person
        # Setting the default value directly as empty set would share the same set among all the instances of the class
        self.tool = set() if tool is None else tool
        self.start = start
        self.end = end
        # Since the relation is homonymous with the method accessing it, it is declared as internal
        # and renamed via the property decorator
        self._refersTo = refersTo

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}('
            f'institute={self.institute!r}, '
            f'person={self.person!r}, '
            f'tool={self.tool!r}, '
            f'start={self.start!r}, '
            f'end={self.end!r}, '
            f'refersTo={self._refersTo!r})'
        )

    def __rich_repr__(self):
        yield int(self.refersTo.identifier) if self.refersTo.identifier.isdigit() else self.refersTo.identifier
        yield "institute", self.institute
        yield "person", self.person
        yield "tool", self.tool
        yield "start", self.start
        yield "end", self.end

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return (self.institute == other.institute and
                self.person == other.person and
                frozenset(self.tool) == frozenset(other.tool) and
                self.start == other.start and
                self.end == other.end and
                self._refersTo == other._refersTo)
    
    def __lt__(self, other) -> bool:
        if self._refersTo != other._refersTo:
            return self._refersTo < other._refersTo
        else:
            return rank[self.__class__.__name__] < rank[other.__class__.__name__]

    def __hash__(self):
        return hash((
            self.institute,
            self.person,
            frozenset(self.tool),
            self.start,
            self.end,
            self._refersTo))

    def getResponsibleInstitute(self) -> str:
        return self.institute
    
    def getResponsiblePerson(self) -> Union[str, None]:
        return self.person
    
    def getTools(self) -> Set[str]:
        return self.tool
    
    def getStartDate(self) -> Union[str, None]:
        return self.start
    
    def getEndDate(self) -> Union[str, None]:
        return self.end
    
    @property
    def refersTo(self) -> CulturalHeritageObject:
        return self._refersTo

    @refersTo.setter
    def refersTo(self, value: CulturalHeritageObject) -> None:
        self._refersTo = value


class Acquisition(Activity):
    def __init__(
        self,
        refersTo: CulturalHeritageObject,
        technique: str,
        institute: str,
        person: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        tool: Optional[set[str]] = None,
    ):
        super().__init__(refersTo, institute, person, start, end, tool)
        self.technique = technique

    def __repr__(self) -> str:
        return (
            f'Acquisition('
            f'institute={self.institute!r}, '
            f'person={self.person!r}, '
            f'technique={self.technique!r}, '
            f'tool={self.tool!r}, '
            f'start={self.start!r}, '
            f'end={self.end!r}, '
            f'refersTo={self._refersTo!r})'
        )

    def __rich_repr__(self):
        yield int(self.refersTo.identifier) if self.refersTo.identifier.isdigit() else self.refersTo.identifier
        yield "institute", self.institute
        yield "person", self.person
        yield "technique", self.technique
        yield "tool", self.tool
        yield "start", self.start
        yield "end", self.end

    def __eq__(self, other) -> bool:
        return super().__eq__(other) and self.technique == other.technique

    def __hash__(self):
        return hash((
            self.institute,
            self.person,
            self.technique,
            frozenset(self.tool),
            self.start,
            self.end,
            self._refersTo))

    def getTechnique(self) -> str:
        return self.technique


class Processing(Activity):
    pass


class Modelling(Activity):
    pass


class Optimising(Activity):
    pass


class Exporting(Activity):
    pass