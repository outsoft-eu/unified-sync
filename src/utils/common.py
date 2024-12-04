import inspect

import traceback

from sqlalchemy import create_engine
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.dialects import registry
from src.utils import sqlalchemy_custom_dialects
from src.utils.sqlalchemy_custom_dialects import CustomDialect


def commit_session(session: Session, _) -> bool:
    """
    Commit each object in session or rollback changes
    :param _:
    :param session: session to commit
    :return: status of commit
    """
    try:
        session.commit()
        return True
    except Exception as ex:
        # TODO add different exception types
        print(f"Exception while committing: {str(ex)}! Rolling back", extra={'trace': traceback.format_exc()})
        session.rollback()
        return False


def register_custom_sqlalchemy_dialects():
    """
        Registers custom SQLAlchemy dialects with the SQLAlchemy registry.

        This function inspects the `sqlalchemy_custom_dialects` module to find all classes that are
        subclasses of `CustomDialect`. For each such class, it attempts to load the dialect from the
        SQLAlchemy registry. If the dialect is not already registered, it registers the dialect.

        Prints a message indicating whether the dialect was already registered or if it was newly registered.
    """
    for name, obj in inspect.getmembers(sqlalchemy_custom_dialects, inspect.isclass):
        if issubclass(obj, CustomDialect) and obj is not CustomDialect:
            try:
                registry.load(obj.reg_name)
                print(f"Dialect {obj.reg_name} ({obj.__name__}) is already registered.")
            except NoSuchModuleError:
                print(f"Registering {obj.reg_name} ({obj.__name__}) in sqlalchemy registry.")
                registry.register(obj.reg_name, sqlalchemy_custom_dialects.__name__, obj.__name__)


def create_session_new(conf: dict, statement_timeout: int = None, schema_translate_map: bool = False) -> Session:
    """
    Function create engine's session for different configuration
    @param conf: input parameters for session creation
    @param statement_timeout: Maximum session timeout state in millis
    @param schema_translate_map:
    :return: Session object
    """
    register_custom_sqlalchemy_dialects()
    if statement_timeout:
        connect_args = {"options": f"-c statement_timeout={statement_timeout}"}
    else:
        connect_args = {}
    engine = create_engine('{}://{}:{}@{}:{}/{}'.format(
        conf['engine_type'],
        conf['user'],
        conf['password'],
        conf['host'],
        conf['port'],
        conf['dbname']),
        echo=False, connect_args=connect_args)
    if schema_translate_map:
        engine = engine.execution_options(schema_translate_map={None: conf['schema'],
                                                                "public": conf['schema']})
    print(f'Session created {engine}')
    return sessionmaker(bind=engine)()
