from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session
from sqlalchemy.orm import aliased


def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result

def select_02():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects sub ON g.subject_id = sub.id
    WHERE sub.name = 'idea'
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).join(Subject).filter(Subject.name == 'idea') \
        .group_by(Student.id).order_by(desc('average_grade')).limit(1).all()
    return result


def select_03():
    """
    SELECT
        g.name,
        AVG(gg.grade) AS avg_grade
    FROM groups g
    JOIN students s ON g.id = s.group_id
    JOIN grades gg ON s.id = gg.student_id
    JOIN subjects sub ON gg.subject_id = sub.id
    WHERE sub.name = 'idea'
    GROUP BY g.name;
    """
    result = session.query(Group.name, func.avg(Grade.grade).label('avg_grade')) \
        .select_from(Group).join(Student).join(Grade).join(Subject).filter(Subject.name == 'idea') \
        .group_by(Group.name).all()
    return result

def select_04():
    """
    SELECT AVG(grade) AS avg_grade FROM grades;
    """
    result = session.query(func.avg(Grade.grade).label('avg_grade')).one()
    return result


def select_05():
    """
    SELECT subjects.name
    FROM subjects
    JOIN teachers ON subjects.teacher_id = teachers.id
    WHERE teachers.fullname = 'Eric Todd';
    """
    result = session.query(Subject.name).join(Teacher).filter(Teacher.fullname == 'Eric Todd').all()
    return result


def select_06():
    """
    SELECT * FROM students WHERE group_id = '3';
    """
    result = session.query(Student.id, Student.fullname).filter(Student.group_id == 3).all()
    return result


def select_07():
    """
    SELECT students.fullname, grades.grade
    FROM students
    JOIN grades ON students.id = grades.student_id
    JOIN subjects ON grades.subject_id = subjects.id
    WHERE students.group_id = '1' AND subjects.name = 'idea';
    """
    result = session.query(Student.fullname, Grade.grade) \
        .join(Grade).join(Subject).filter(and_(Student.group_id == 1, Subject.name == 'idea')).all()
    return result


def select_08():
    """
    SELECT teachers.fullname, AVG(grades.grade) AS avg_grade
    FROM teachers
    JOIN subjects ON teachers.id = subjects.teacher_id
    JOIN grades ON subjects.id = grades.subject_id
    GROUP BY teachers.fullname;
    """
    teacher_alias = aliased(Teacher)
    result = session.query(teacher_alias.fullname, func.avg(Grade.grade).label('avg_grade')) \
        .select_from(teacher_alias).join(Subject, teacher_alias.id == Subject.teacher_id) \
        .join(Grade, Subject.id == Grade.subjects_id) \
        .group_by(teacher_alias.fullname).all()
    return result


def select_09():
    """
    SELECT subjects.name
    FROM subjects
    JOIN grades ON subjects.id = grades.subject_id
    JOIN students ON grades.student_id = students.id
    WHERE students.fullname = 'Christopher Combs IV';
    """
    result = session.query(Subject.name).join(Grade).join(Student) \
        .filter(Student.fullname == 'Christopher Combs IV').all()
    return result


def select_10():
    """
    SELECT subjects.name
    FROM subjects
    JOIN teachers ON subjects.teacher_id = teachers.id
    JOIN grades ON subjects.id = grades.subject_id
    JOIN students ON grades.student_id = students.id
    WHERE students.fullname = 'William Wood' AND teachers.fullname = 'Barbara Blackwell';
    """
    result = session.query(Subject.name) \
        .join(Teacher).join(Grade).join(Student) \
        .filter(and_(Student.fullname == 'William Wood', Teacher.fullname == 'Barbara Blackwell')).all()
    return result






if __name__ == '__main__':
    # print(select_01())
    # print(select_02())
    # print(select_03())
    # print(select_04())
    # print(select_05())
    # print(select_06())
    # print(select_07())
    # print(select_08())
    # print(select_09())
    print(select_10())