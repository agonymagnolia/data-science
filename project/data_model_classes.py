class IdentifiableEntity(object):
    def __init__(self, identifier: str) -> None:
        self.identifier = identifier
    
    def getId(self) -> str:
        return self.identifier


class Person(IdentifiableEntity):
    def __init__(self, identifier: str, name: str) -> None:
        super().__init__(identifier=identifier)
        self.name = name

    def __repr__(self) -> str:
        return f'Person(id={self.identifier!r}, name={self.name!r})'

    def getName(self) -> str:
        return self.name


class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, identifier: str, title: str, owner: str, place: str, date: str | None = None, hasAuthor: list[Person] | None = None) -> None:
        super().__init__(identifier=identifier)
        self.title = title
        self.owner = owner
        self.place = place
        self.date = date
        self.hasAuthor = list() if hasAuthor is None else hasAuthor # Otherwhise the empty list set as default value is shared among all the instances of the class

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(id={self.identifier!r}, title={self.title!r}, owner={self.owner!r}, place={self.place!r}, date={self.date!r}, hasAuthor={self.hasAuthor!r})'

    def getTitle(self) -> str:
        return self.title
    
    def getDate(self) -> str | None:
        return self.date
        
    def getOwner(self) -> str:
        return self.owner
    
    def getPlace(self) -> str:
        return self.place

    def getAuthors(self) -> list[Person]:
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


class Activity(object):
    def __init__(self, refersTo: CulturalHeritageObject, institute: str, person: str | None = None, tool: set[str] | None = None, start: str | None = None, end: str | None = None):
        self.institute = institute
        self.person = person
        self.tool = set() if tool is None else tool
        self.start = start
        self.end = end
        self._refersTo = refersTo

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(institute={self.institute!r}, person={self.person!r}, tool={self.tool!r}, start={self.start!r}, end={self.end!r}, refersTo={self._refersTo!r})'
    
    def getResponsibleInstitute(self) -> str:
        return self.institute
    
    def getResponsiblePerson(self) -> str | None:
        return self.person
    
    def getTools(self) -> set[str]:
        return self.tool
    
    def getStartDate(self) -> str | None:
        return self.start
    
    def getEndDate(self) -> str | None:
        return self.end
    
    @property
    def refersTo(self) -> CulturalHeritageObject:
        return self._refersTo

    @refersTo.setter
    def refersTo(self, value) -> None:
        self._refersTo = value


class Acquisition(Activity):
    technique: str

    def __init__(self, refersTo: CulturalHeritageObject, technique: str, institute: str, person: str | None = None, tool: set[str] = set(), start: str | None = None, end: str | None = None):
        super().__init__(refersTo, institute, person, tool, start, end)
        self.technique = technique

    def __repr__(self) -> str:
        return f'Acquisition(institute={self.institute!r}, person={self.person!r}, technique={self.technique!r}, tool={self.tool!r}, start={self.start!r}, end={self.end!r}, refersTo={self._refersTo!r})'

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