import pandas as pd
from talib import ATR
import datetime
import threading

RAW_DIRECTORY = 'raw'
OUT_DIRECTORY = 'out'

def construct_result_dict(feature_window_size):
    result = dict()
    result['label'] = []
    result['day'] = []
    result['time'] = []
    for i in range(feature_window_size):
        result['tickVolume_'+str(i)] = []
        result['close_'+str(i)] = []

    return result
    
def transform(feature_window_size=30, future_window_size_for_label=15, filename='GBPCHF_H4', tp_scale=1, sl_scale=1.5):
    print('transforming',filename, feature_window_size, future_window_size_for_label, label_criteria_scale)
    raw = pd.read_csv(RAW_DIRECTORY + '/' + filename);
    atr = ATR(raw['high'],raw['low'],raw['close'])
    result_dict = construct_result_dict(feature_window_size)

    for i_raw_row in range(raw.shape[0]):
        if i_raw_row % 100 == 0:
            print(filename,':',i_raw_row,'/',raw.shape[0])

        if i_raw_row + future_window_size_for_label + 1 >= raw.shape[0]:
            break
        if i_raw_row < feature_window_size:
            continue

        row = raw.iloc[i_raw_row]
        current_close = row['close']
        
        atr_val = atr[i_raw_row]
        up_tp = current_close + atr_val*tp_scale
        down_tp = current_close - atr_val*tp_scale
        up_sl = current_close - atr_val*sl_scale
        down_sl = current_close + atr_val*sl_scale

        label = 'none'
        hit_up_tp = None
        hit_down_tp = None
        hit_up_sl = None
        hit_down_sl = None
        for i_future in range(1, future_window_size_for_label+1):
    ##        print(i_future, i_raw_row+i_future, raw.shape[0])
            new_row = raw.iloc[i_raw_row+i_future]
            high = new_row['high']
            low = new_row['low']

            if high >= up_tp:
                hit_up_tp = i_future
            if high >= down_sl:
                hit_down_sl = i_future
            if low <= up_sl:
                hit_up_sl = i_future
            if low <= down_tp:
                hit_down_tp = i_future

        should_up = False
        should_down = False
        if hit_up_tp != None and (hit_up_sl is None or hit_up_sl - hit_up_tp >= 5) :
            should_up = True
        if hit_down_tp != None and (hit_down_sl is None or hit_down_sl - hit_down_tp >= 5) :
            should_down = True
        if should_up and should_down:
            label = 'none'
        elif should_up:
            label = 'up'
        elif should_down:
            label = 'down'

        date = row['date'].split('.')
        date = datetime.datetime(int(date[0]),int(date[1]),int(date[2]))
        day_of_week = 'day-'+str(date.weekday())
        
        result_dict['label'].append(label)
        result_dict['day'].append(day_of_week)
        result_dict['time'].append(row['time'])
                
        for i_bar in range(feature_window_size):
            new_row = raw.iloc[i_raw_row-i_bar]
            result_dict['close_'+str(i_bar)].append(new_row['close'])
            result_dict['tickVolume_'+str(i_bar)].append(new_row['tickVolume'])

    result_df = pd.DataFrame(result_dict)
    target_filename = filename + '_' + str(feature_window_size) + '_' + str(future_window_size_for_label)
    target_filename += '.csv'
    target_filepath = OUT_DIRECTORY + '/' + target_filename
    print('saving csv file ' + target_filepath)
    result_df.to_csv(target_filepath,index=False)
    print('saving csv done')

##a = threading.Thread(target=transform, kwargs={
##    'feature_window_size':120,
##    'future_window_size_for_label':15,
##    'filename':'AUDJPY_H4.csv',
##    'label_criteria_scale':1,
##    'include_open_high_low':False
##})
##a.start()

b = threading.Thread(target=transform, kwargs={
    'feature_window_size':120,
    'future_window_size_for_label':18,
    'filename':'AUDJPY_H1.csv',
    'tp_scale':1.7,
    'sl_scale':1.5,
})
b.start()

##a.join()
b.join()
