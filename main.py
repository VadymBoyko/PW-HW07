from sqlalchemy import func, desc, select, and_

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_01():
    """
    Найти 5 студентов с наибольшим средним баллом по всем предметам.
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_02(discipline_id: int):
    """"
    Найти студента с наивысшим средним баллом по определенному предмету
    """
    r = session.query(Discipline.name,
                      Student.fullname,
                      func.round(func.avg(Grade.grade), 2).label('avg_grade')
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Student.id, Discipline.name) \
        .order_by(desc('avg_grade')) \
        .limit(1).all()
    return r

def select_03(discipline_id: int):
    """"
    Найти средний балл в группах по определенному предмету
    """
    r = session.query(Discipline.name,
                      Group.name,
                      func.round(func.avg(Grade.grade), 2).label('avg_grade')
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Group) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Group.id, Discipline.name) \
        .order_by(desc('avg_grade')) \
        .all()
    return r


def select_04():
    """"
    Найти средний балл на потоке (по всей таблице оценок)
    """
    r = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade')).select_from(Grade).all()
    return r


def select_05(teacher_id: int):
    """"
    Найти какие курсы читает определенный преподаватель
    """
    r = session.query(Discipline.name).select_from(Discipline).join(Teacher).filter(Teacher.id == teacher_id).all()
    return r


def select_06(group_id: int):
    """"
    Найти список студентов в определенной группе
    """
    r = session.query(Student.fullname).select_from(Student).join(Group).filter(Group.id == group_id).all()
    return r


def select_07(group_id, discipline_id: int):
    """"
    Найти оценки студентов в отдельной группе по определенному предмету.
    """
    r = session.query(Discipline.name,
                      Group.name,
                      Grade.grade
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Group) \
        .join(Discipline) \
        .filter(and_(Group.id == group_id, Discipline.id == discipline_id)) \
        .all()
    return r


def select_08(teacher_id: int):
    """"
    Найти средний балл, который ставит определенный преподаватель по своим предметам
    """
    r = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Discipline) \
        .join(Teacher) \
        .filter(Teacher.id == teacher_id) \
        .all()
    return r


def select_09(student_id: int):
    """"
    Найти список курсов, которые посещает определенный студент
    """
    r = session.query(Discipline.name) \
        .select_from(Grade) \
        .join(Discipline) \
        .join(Student) \
        .filter(Student.id == student_id)\
        .distinct(Discipline.name).all()
    return r


def select_10(student_id, teacher_id: int):
    """"
    Список курсов, которые определенному студенту читает определенный преподаватель
    """
    r = session.query(Discipline.name) \
        .select_from(Grade) \
        .join(Discipline) \
        .join(Student) \
        .join(Teacher) \
        .filter(and_(Student.id == student_id, Teacher.id == teacher_id)) \
        .distinct(Discipline.name).all()
    return r


def select_11(student_id, teacher_id: int):
    """"
    Средний балл, который определенный преподаватель ставит определенному студенту
    """
    r = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Discipline) \
        .join(Student) \
        .join(Teacher) \
        .filter(and_(Student.id == student_id, Teacher.id == teacher_id)) \
        .all()
    return r


def select_12(discipline_id, group_id):
    """
    Оценки студентов в определенной группе по определенному предмету на последнем занятии.
    """
    subquery = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == discipline_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1).scalar_subquery())

    r = session.query(Discipline.name,
                      Student.fullname,
                      Group.name,
                      Grade.date_of,
                      Grade.grade
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group)\
        .filter(and_(Discipline.id == discipline_id, Group.id == group_id, Grade.date_of == subquery)) \
        .order_by(desc(Grade.date_of)) \
        .all()
    return r


if __name__ == '__main__':
    print(select_01())
    print(select_02(1))
    print(select_03(1))
    print(select_04())
    print(select_05(4))
    print(select_06(2))
    print(select_07(3, 8))
    print(select_08(3))
    print(select_09(1))
    print(select_10(2, 1))
    print(select_11(5, 1))
    print(select_12(1, 2))