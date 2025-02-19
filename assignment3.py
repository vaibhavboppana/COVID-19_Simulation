import pandas as pd
import numpy as np
import helper as h
import sim_parameters as s



def m_chain(df_sample_population,temp_time_series,start_date):
    """
    This function uses the markov chain model and generate the state of the person based on the probability.
    """
    for id,record in enumerate(df_sample_population):
        prev_state_temp,prev_staying_days_temp,hoding_day='H',0,[]
        temp=s.TRASITION_PROBS[record[1]]
        for temp_date in temp_time_series:
            if temp_date==start_date:
                state=prev_state_temp
                staying_days=prev_staying_days_temp
                prev_state=prev_state_temp
            else:
                if len(hoding_day)==0:
                    temp1=temp[prev_state]
                    state=np.random.choice(list(temp1.keys()), p=list(temp1.values()))
                    hoding_day=list(range(1,s.HOLDING_TIMES[record[1]][state]))[::-1]
                    staying_days=0
                elif len(hoding_day)>0:
                    state=prev_state_temp
                    staying_days=hoding_day.pop()
                prev_state=prev_state_temp
            prev_state_temp,prev_staying_days_temp=state,staying_days
            yield [id,record[1],record[0],temp_date,state,staying_days,prev_state]




def sample_population(countries,df_filtered_countries_details):
    """
    This function generates the dataset of citizens based on the country name and age group
    """
    for  data in countries:
        filter_country=df_filtered_countries_details[df_filtered_countries_details.country==data]
        for age_group in['less_5','5_to_14','15_to_24','25_to_64','over_65']:
            arr_records=[[data,age_group]]*int(filter_country['no_of_records_'+age_group])
            for record in arr_records:
                yield record

def summary_of_state(df_covid_simulated_timeseries):
    """
        This function creates the new columns of deceased, healthy, infected without symptoms,infected with symptoms,
         immunity cases
    """
    df_=df_covid_simulated_timeseries[['date','country','state']]
    dic_temp={'D':0,'H':1,'I':2,'M':3,'S':4}
    for i in df_.itertuples():
        record=[0,0,0,0,0]
        record[dic_temp[i[-1]]]=1
        yield record


def run(countries_csv_name,countries,sample_ratio,start_date,end_date):
    df_countries_details=h.read_dataset(countries_csv_name)
    df_filtered_countries_details=df_countries_details[df_countries_details.country.isin(countries)] #filtering  the countries  based on input 
    sample_ratio_of_each_country=[int(i//sample_ratio) for i in df_filtered_countries_details.population]

    df_filtered_countries_details['no_of_records']=sample_ratio_of_each_country
    df_filtered_countries_details['no_of_records_less_5']=df_filtered_countries_details['no_of_records']*df_filtered_countries_details['less_5']*10**-2
    df_filtered_countries_details['no_of_records_5_to_14']=df_filtered_countries_details['no_of_records']*df_filtered_countries_details['5_to_14']*10**-2
    df_filtered_countries_details['no_of_records_15_to_24']=df_filtered_countries_details['no_of_records']*df_filtered_countries_details['15_to_24']*10**-2
    df_filtered_countries_details['no_of_records_25_to_64']=df_filtered_countries_details['no_of_records']*df_filtered_countries_details['25_to_64']*10**-2
    df_filtered_countries_details['no_of_records_over_65']=df_filtered_countries_details['no_of_records']*df_filtered_countries_details['over_65']*10**-2

    df_sample_population=sample_population(countries,df_filtered_countries_details)
    temp_time_series=pd.date_range(start=start_date, end=end_date)
    temp_time_series=[i.strftime(r'%Y-%m-%d') for i in temp_time_series]
    covid_simulated_timeseries=m_chain(df_sample_population,temp_time_series,start_date)
    df_covid_simulated_timeseries=pd.DataFrame(covid_simulated_timeseries,columns=['person_id','age_group_name','country','date','state','staying_days','prev_state'])
    df_covid_simulated_timeseries.to_csv('a3-covid-simulated-timeseries.csv',index=False)
    arr_summary_of_state=summary_of_state(df_covid_simulated_timeseries)
    df_summary_of_state=pd.DataFrame(arr_summary_of_state,columns=['D','H','I','M','S'])
    df_covid_simulated_timeseries[['D','H','I','M','S']]=df_summary_of_state
    
    df_covid_summary_timeseries=df_covid_simulated_timeseries.drop(columns=['state','person_id','staying_days'])
    df_covid_summary_timeseries.groupby(['date','country']).sum().reset_index().to_csv('a3-covid-summary-timeseries.csv',index=False)
    h.create_plot('a3-covid-summary-timeseries.csv',countries)

if __name__=='__main__':
    run('./a3-countries.csv',['Afghanistan','Sweden','Japan'],1e6,'2021-04-01','2022-04-28')