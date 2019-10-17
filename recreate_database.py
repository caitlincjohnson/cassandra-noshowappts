import logging
import etl_utilities as etl


logging.basicConfig(level=logging.INFO)


def main():
    """
    Main script that tears down and rebuilds the tables within Cassandra
    """
    cluster, session = etl.cassandra_connection()

    try:
        table_list = ['appointments', 'patients']

        for table in table_list:
            drop_table = 'DROP TABLE IF EXISTS {table}'.format(table=table)
            session.execute(drop_table)

        create_appointments_table = "CREATE TABLE IF NOT EXISTS appointments "\
                                    "(AppointmentId int" \
                                    ", PatientId text" \
                                    ", ScheduledDay timestamp" \
                                    ", AppointmentDay timestamp" \
                                    ", SMSReceived boolean" \
                                    ", NoShow text" \
                                    ", PRIMARY KEY (AppointmentId))"
        logging.info('Creating appointments table in Cassandra')
        session.execute(create_appointments_table)

        create_patients_table = "CREATE TABLE IF NOT EXISTS patients "\
                                "(PatientId text" \
                                ", Gender text" \
                                ", Age int" \
                                ", Hypertension boolean" \
                                ", Diabetes boolean" \
                                ", Alcoholism boolean" \
                                ", Handicap boolean" \
                                ", State text" \
                                ", PRIMARY KEY (PatientId))"

        logging.info('Creating patients table in Cassandra')
        session.execute(create_patients_table)

    except Exception as e:
        print(e)

    finally:
        logging.info('Closing connection to Cassandra')
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()