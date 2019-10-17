import pandas as pd
import os
import glob
import logging
import numpy as np
import etl_utilities as etl


logging.basicConfig(level=logging.INFO)


def transform(df):
    """
    Perform transformation on the data, including the addition of a State column and conversion of bit values to boolean
    :param df: Pandas Dataframe
    :return: Transformed Pandas Dataframe
    """
    # dropping unnecessary columns
    df = df.drop(labels=['Neighborhood', 'Scholarship'], axis=1)

    # adding a column with states randomly selected from a predefined list
    state_list = ['AL', 'GA', 'MD', 'TX', 'CO', 'CA', 'FL', 'DC', 'VA']
    df['State'] = np.random.choice(state_list, size=len(df))

    # replace 0s and 1s with False and True to be more readable
    df.replace([0, 1], [False, True], inplace=True)
    df.replace(['F', 'M'], ['Female', 'Male'], inplace=True)

    # renaming the columns to have correct spelling and consistent naming convention
    df = df.rename(columns={
        'Handcap': 'Handicap'
        , 'SMS_Received': 'SMSReceived'
        , 'No_Show': 'NoShow'
    })

    # converting date columns from object to datetime
    df['ScheduledDay'] = pd.to_datetime(df['ScheduledDay'])
    df['AppointmentDay'] = pd.to_datetime(df['AppointmentDay'])
    return df


def main():
    """
    Main script that performs the ETL
    """
    logging.info('Connecting to Cassandra')
    session, cluster = etl.cassandra_connection()

    try:
        logging.info('Importing data into Pandas Dataframe that will be used for Cassandra tables')
        files = glob.glob(os.path.join(os.getcwd() + '/data/', '*.csv'))

        # adding dtype of object for PatientId to prevent Pandas from converting it to a float
        separate_dfs = (pd.read_csv(file, dtype={'PatientId': object}) for file in files)
        df = pd.concat(separate_dfs, ignore_index=True)

        logging.info('Transforming the data')
        df = transform(df)

        logging.info('Loading data into the tables')
        query_insert_patients = "INSERT INTO patients " \
                                "(PatientId, Gender, Age, Hypertension, Diabetes, Alcoholism, Handicap, State) " \
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        prepared_patients = session.prepare(query_insert_patients)
        query_insert_appointments = "INSERT INTO appointments " \
                                    "(AppointmentID, PatientId, ScheduledDay, AppointmentDay, SMSReceived, NoShow)" \
                                    " VALUES (?, ?, ?, ?, ?, ?)"
        prepared_appointments = session.prepare(query_insert_appointments)

        for index, row in df.iterrows():
            session.execute(prepared_patients
                            , (row['PatientId']
                               , row['Gender']
                               , row['Age']
                               , row['Hypertension']
                               , row['Diabetes']
                               , row['Alcoholism']
                               , row['Handicap']
                               , row['State']))
            session.execute(prepared_appointments
                            , (row['AppointmentID']
                               , row['PatientId']
                               , row['ScheduledDay']
                               , row['AppointmentDay']
                               , row['SMSReceived']
                               , row['NoShow']))

    except Exception as e:
        print(e)

    finally:
        logging.info('Closing connection to Cassandra')
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
