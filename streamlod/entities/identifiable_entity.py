from typing import Union, Optional, List

from streamlod.utils import key

class IdentifiableEntity:
    def __init__(self, identifier: str):
        self.identifier = identifier

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.identifier == other.identifier

    def getId(self) -> str:
        return self.identifier


class Person(IdentifiableEntity):
    def __init__(self, identifier: str, name: str):
        super().__init__(identifier)
        self.name = name

    def __repr__(self) -> str:
        return f'Person(id={self.identifier!r}, name={self.name!r})'

    def __rich_repr__(self):
        yield self.name
        yield "id", self.identifier

    def __eq__(self, other) -> bool:      
        return (super().__eq__(other) and
                self.name == other.name)

    def __lt__(self, other) -> bool:
        return self.name < other.name

    def __hash__(self):
        return hash((self.identifier, self.name))

    def getName(self) -> str:
        return self.name


class CulturalHeritageObject(IdentifiableEntity):
    def __init__(
        self,
        identifier: str,
        title: str,
        owner: str,
        place: str,
        date: Optional[str] = None,
        hasAuthor: Optional[List[Person]] = None
    ):
        super().__init__(identifier)
        self.title = title
        self.owner = owner
        self.place = place
        self.date = date
        # Setting the default value directly as empty list would share the same list among all the instances of the class
        self.hasAuthor = [] if hasAuthor is None else hasAuthor

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}('
            f'id={self.identifier!r}, '
            f'title={self.title!r}, '
            f'owner={self.owner!r}, '
            f'place={self.place!r}, '
            f'date={self.date!r}, '
            f'hasAuthor={self.hasAuthor!r})'
        )

    def __rich_repr__(self):
        yield self.title
        yield "id", int(self.identifier) if self.identifier.isdigit() else self.identifier
        yield "owner", self.owner
        yield "place", self.place
        yield "date", self.date
        yield "hasAuthor", self.hasAuthor

    def __eq__(self, other) -> bool:
        return (super().__eq__(other) and
                self.title == other.title and
                self.owner == other.owner and
                self.place == other.place and
                self.date == other.date and
                tuple(self.hasAuthor) == tuple(other.hasAuthor))

    def __lt__(self, other) -> bool:
        # Numeric and alphabetic identifiers are placed respecitvely in a 0 or 1 tuple to allow for comparison
        return key(self.identifier) < key(other.identifier)

    def __hash__(self):
        return hash((
            self.identifier,
            self.title,
            self.owner,
            self.place,
            self.date,
            tuple(self.hasAuthor)))

    def getTitle(self) -> str:
        return self.title
    
    def getDate(self) -> Union[str, None]:
        return self.date
        
    def getOwner(self) -> str:
        return self.owner
    
    def getPlace(self) -> str:
        return self.place

    def getAuthors(self) -> List[Person]:
        return self.hasAuthor


class NauticalChart(CulturalHeritageObject):
    pass


class ManuscriptPlate(CulturalHeritageObject):
    pass


class ManuscriptVolume(CulturalHeritageObject):
    pass


class PrintedVolume(CulturalHeritageObject):
    pass


class PrintedMaterial(CulturalHeritageObject):
    pass


class Herbarium(CulturalHeritageObject):
    pass


class Specimen(CulturalHeritageObject):
    pass


class Painting(CulturalHeritageObject):
    pass


class Model(CulturalHeritageObject):
    pass


class Map(CulturalHeritageObject):
    pass