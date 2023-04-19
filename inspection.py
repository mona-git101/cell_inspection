import pandas as pd
import numpy as np
from scipy.spatial import distance


class calibrate(object):

    def __init__(self , calibration_parameter):
        self.num_reference = calibration_parameter['num_reference_training']
        self.num_target = calibration_parameter['num_target_training']

    def get_average_eis(self , data_dict , num_training):
        df_sum = {}
        df_avg = {}

        for i in data_dict.keys():
            for k in range(num_training):
                if k == 0:
                    df_sum[i] = data_dict[i][k]
                else:
                    df_sum[i] = df_sum[i] + data_dict[i][k]

        for k in df_sum.keys():
            df_avg[k] = df_sum[k] / num_training

    def get_del(self , avg_ref , avg_target):
        delta = {}
        for k in avg_ref.keys():
            delta[k] = avg_ref[k] - avg_target[k]

        calibration_model = {
            'zre': delta['zre'] ,
            '-zim': delta['-zim']
        }
        return calibration_model

    def calibration_training(self , reference , target , calibration_parameter):

        reference_df_avg = self.get_average_eis(reference , calibration_parameter['num_reference_training'])
        target_df_avg = self.get_average_eis(target , calibration_parameter['num_target_training'])

        calibration_model = self.get_del(reference_df_avg , target_df_avg)

        return calibration_model , reference_df_avg

    def calibration(self , target , delta):
        calibrated = {
            'zre': target['zre'] + delta['zre'] ,
            '-zim': target['-zim'] + delta['-zim']
        }

        return calibrated


class diagnose(object):

    def __init__(self , diagnosis_parameter):
        self.statistical_threshold = diagnosis_parameter['statistical_threshold']
        self.safety_factor = diagnosis_parameter['safety_factor']
        self.determinant_limit = diagnosis_parameter['determinant_limit']
        self.ohmic_threshold = diagnosis_parameter["ohmic_threshold"]
        self.num_calibration_error_check = diagnosis_parameter['num_calibration_error_check']
        self.calibration_error_percent_threshold = diagnosis_parameter['calibration_error_percent_threshold']

    def convert_feature(self , calibrated):

        input_features = {
            'p1': np.array([calibrated['zre'][0] , calibrated['-zim'][0]]) ,
            'p2': np.array([calibrated['zre'][1] - calibrated['zre'][0] , calibrated['-zim'][1]]) ,
            'p3': np.array([calibrated['zre'][2] - calibrated['zre'][0] , calibrated['-zim'][2]])
        }

        return input_features

    def diagnosis_training(self , input_features_list):

        learned_model = {}

        for k in input_features_list.keys():
            if k == 'p1':
                learned_model[k] = {
                    'learned_mean': np.mean(np.transpose(input_features_list[k])[0]) ,
                    'learned_std': np.std(np.transpose(input_features_list[k])[0] , ddof=1)
                }

            else:
                learned_model[k] = {
                    'learned_mean_zre': np.mean(np.transpose(input_features_list[k])[0]) ,
                    'learned_mean_-zim': np.mean(np.transpose(input_features_list[k])[1]) ,
                    'learned_cov': np.cov(np.transpose(input_features_list[k]) , ddof=1)
                }

        return learned_model

    def statistical_calculation(self , input_features , learned_model):
        statistical_distances = []

        for k in input_features.keys():
            if k == 'p1':
                md = (input_features[k][0] - learned_model['p1']['learned_mean']) / learned_model['p1']['learned_std']

            else:
                input_feature = np.array(input_features[k])

                if np.linalg.det(learned_model[k]['learned_cov']) < self.determinant_limit:
                    md = 0

                else:
                    learned_mean = np.array([learned_model[k]['learned_mean_zre'] , learned_model[k]['learned_mean_-zim']])
                    inverse_cov = np.linalg.inv(learned_model[k]['learned_cov'])
                    md = (distance.mahalanobis(np.array(input_feature) , learned_mean , inverse_cov)) / np.sqrt(len(input_features))

            statistical_distances.append(md)

        return np.array(statistical_distances)

    def diagnosis(self , input_features , historical_alarm , learned_model):
        diagnosis_threshold = self.statistical_threshold * self.safety_factor

        statistical_distances = self.statistical_calculation(input_features , learned_model)
        diagnosis_alarm_index_temp = np.abs(statistical_distances) >= diagnosis_threshold
        diagnosis_alarm_index = []
        for i in diagnosis_alarm_index_temp:
            diagnosis_alarm_index.append(int(i))
        diagnosis_alarm = 1
        ohmic_alarm = 0

        if sum(diagnosis_alarm_index) == 0:
            diagnosis_alarm = 0
        if historical_alarm.tolist().count(1) >= self.num_calibration_error_check * self.calibration_error_percent_threshold:
            calibration_alarm = 1
        else:
            calibration_alarm = 0

        if np.abs(input_features['p1'][1]) >= self.ohmic_threshold:
            ohmic_alarm = 1

        diagnosis_result = {
            'statistical_distances': statistical_distances ,
            'diagnosis_alarm': diagnosis_alarm ,
            'diagnosis_alarm_index': diagnosis_alarm_index ,  # modified on 230104
            'calibration_alarm': calibration_alarm ,
            'ohmic_alarm': ohmic_alarm
        }

        return diagnosis_result


class inspect:

    def __init__(self , device , calibration_parameter , diagnosis_parameter):
        self.device = device
        self.calibration_parameter = calibration_parameter
        self.diagnosis_parameter = diagnosis_parameter
        self.cali = calibrate(calibration_parameter)
        self.diag = diagnose(diagnosis_parameter)

    def calibration_training(self , references , targets):
        calibration_model , reference_avg = self.cali.calibration_training(references , targets , self.calibration_parameter)

        return calibration_model , reference_avg

    def diagnosis_training(self , learn_inputs , calibration_model):
        aa = []
        bb = []
        cc = []

        for k in range(self.diagnosis_parameter['num_training']):
            inputs = {
                'zre': learn_inputs['zre'][k] ,
                '-zim': learn_inputs['-zim'][k]
            }

            calibrated = self.cali.calibration(inputs , calibration_model)
            feature = self.diag.convert_feature(calibrated)

            aa.append(feature['p1'])
            bb.append(feature['p2'])
            cc.append(feature['p3'])

        input_features = {
            'p1': np.array(aa) ,
            'p2': np.array(bb) ,
            'p3': np.array(cc)
        }

        diagnosis_model = self.diag.diagnosis_training(input_features)

        return diagnosis_model

    def inspection(self , input , historical_alarm , calibration_model , diagnosis_model):
        calibrated = self.cali.calibration(input , calibration_model)
        input_features = self.diag.convert_feature(calibrated)
        diag_result = self.diag.diagnosis(input_features , historical_alarm , diagnosis_model)

        return calibrated , input_features , diag_result
