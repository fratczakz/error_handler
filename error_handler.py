# error_handler.py
"""
This is a module for handling erro messages generated during the transfer of records from LabDB to Central DB.
It is designed to be a singleton so the same instance is used through the whole script.
It collects:
- validation errors
- database errors
- consistency errors
- records statistics
"""

import os
import re
import logging
import ConfigParser
import itertools
import time

import globals

class ErrorCollection(list):
    """
    Class Collecting errors of certain type (validation/consistency)
    and producing error final message.
        """    
    def __init__(self, *args, **kw):
        super(ErrorCollection, self).__init__(*args, **kw)

    def __group_errors_t_s(self):
        """Method to group errors by target and stage
        Args:
            None
        Returns:
            Grouped dict
        """
        error_dict = {}
        for k, v in itertools.groupby(sorted(self, key=(lambda o: o.target)), (lambda x: x.target)):
            error_dict[k] = {}
            for s, e in itertools.groupby(sorted(list(v), key=(lambda o: o.stage)), (lambda x: x.stage)):
                error_dict[k][s] = list(e)
        return error_dict   

    def __group_errors_s(self):
        """Method to group errors by stage
        Args:
            None
        Returns:
            Grouped dict
        """
        error_dict = {}
        for k, v in itertools.groupby(sorted(self, key=(lambda o: o.stage)), (lambda x: x.stage)):
            error_dict[k] = list(v)
        return error_dict

    def get_error_msg(self):
        """Method returning full error message.
        """
        message = ''
        error_dict = self.__group_errors_t_s()
        error_dict_keys = error_dict.keys()
        for k in sorted(error_dict_keys):
            message += 'Target: %s\n'%(k)
            for stage, items in error_dict[k].items():
                message += '\tStage: %s\t(%s error(s)).\n'%(stage, len(items))
                for item in items:
                    message += '\t\t%s'%(item)
        return message

    def get_error_count_summary(self):
        """Method returning just numbers of of errors in separate stages.
        """
        message = ''
        error_dict = self.__group_errors_s()
        grand_total = 0
        for k, v in error_dict.items():
            errors_count = len(list(v))
            message += '\t%s in %s\n'%(errors_count, k)
            grand_total += errors_count
        return '%s error(s) occured during this run.\n%s'%(grand_total, message)

    def get_error_count_msg(self):
        """Method returning 
        """
        message = ''
        error_dict = self.__group_errors_t_s()
        error_dict_keys = error_dict.keys()
        grand_total = 0
        for k in sorted(error_dict_keys):
            detailed_message = ''
            total = 0
            for stage, items in error_dict[k].items():
                total += len(items)
                detailed_message += '\t%s in %s\n'%(len(items), stage)
            grand_total += total
            message += 'Target: %s\t(%s error(s))\n%s'%(k, total, detailed_message)
        return '%s error(s) occured during this run.\n %s'%(grand_total, message)

class ValidationError(object):
    """Class storing details on the validation error.
    """
    def __init__(self, target, stage, suffix, parameter, value, notice=None):
        # print 'Addind verror %s %s %s %s %s'%(target, stage, suffix, parameter, notice)
        self.target = target
        self.stage = stage
        self.suffix = suffix
        self.parameter = parameter
        self.value = value
        self.notice = notice

    def __str__(self):
        return 'Error: %s, Value of %s = \'%s\' for %sid = \'%s\'\n'%(
            self.notice, self.parameter, self.value, self.stage+"_", self.suffix
            )

class ConsistencyError(object):
    """Class storing details on consistency error.
    """
    def __init__(self, target, stage, suffix):
        self.target = target
        self.stage = stage
        self.suffix = suffix

    def __str__(self):
        return 'Missing suffix: %s\n'%(self.suffix)

class RecordCounter(dict):
    """Class counting appearance of certain value.
    """
    def __init__(self, *args, **kw):
        super(RecordCounter, self).__init__(*args, **kw)

    def increment(self, counter_name):
        if self.has_key(counter_name):
            self[counter_name] += 1
        else:
            self[counter_name] = 1

class DatabaseErrorCollection(RecordCounter):
    """Class counting the database errors and producing final message.
    """
    def __init__(self, *args, **kw):
        super(DatabaseErrorCollection, self).__init__(*args, **kw)

    def get_error_msg(self):
        """Method returning final message.
        """
        if self:
            message = 'Following database errors occured during the run:\n'
            for k, v in self.items():
                message += "In %s recod(s):\n%s\n"%(v, k)
            return message
        return 'There were no database errors during this run.'

    def get_error_count_msg(self):
        """Method returning number of distinct database errors.
        """
        return '%s error(s) occured during run.\n'%(len(self))

class ErrorHandler(object):
    """Main error handling class."""

    # Keeps its instance as a class object.
    _instance = None


    def __init__(self):
        # declaration of inner error collections
        self._validation_error_collection = ErrorCollection()
        self._db_error_collection = DatabaseErrorCollection()
        self._consistency_error_collection = ErrorCollection()
        self._r_counter = {}

        # setup logger for storing additional information
        logFolder = globals.log_folder
        if not os.path.exists(logFolder):
            os.makedirs(logFolder)
            
        self._logger = logging.getLogger("%s_db2db"%(globals.center_name))
        self._logger.setLevel(logging.DEBUG)
        # create file handler which logs even debug messages
        fh = logging.FileHandler("%s%s_db2db-%s.log"%(logFolder, globals.center_name, time.strftime("%Y-%m-%d", time.localtime())), 'a')
        fh.setLevel(logging.DEBUG)

        # create console handler with a higher log level
        # create formatter and add it to the handlers
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)

        # add the handlers to the logger
        self._logger.addHandler(fh)
        self._logger.info('Logger initiated.')

    def attach_db_error(self, text):
        """Method store db error and add it to log for future investigation.
        """
        self._logger.info(text)
        self._db_error_collection.increment(text)

    def attach_consistency_error(self, target, stage, id):
        """Method to store consistency error.
        """
        self._consistency_error_collection.append(ConsistencyError(target, stage, id))

    def attach_validation_error(self, stage, param, data_dict, notice=None):
        """Method to store validation error.
        """
        target = None
        suffix = None
        parameter = param
        value = None
        notice = notice
        stage = stage

        if data_dict.has_key('protein_target_id'):
            target = data_dict['protein_target_id']
        if stage == 'protocol':
            target = 'Protocol'
        if data_dict.has_key('%s_id'%(stage)):
            suffix = data_dict['%s_id'%(stage)]
        if data_dict.has_key(param):
            value = data_dict[param]
        self._validation_error_collection.append(ValidationError(target, stage, suffix, parameter, value, notice))

    # Methods returning multiple types of messages
    def get_validation_error_msg(self):
        return self._validation_error_collection.get_error_msg()

    def get_consistency_error_msg(self):
        return self._consistency_error_collection.get_error_msg()
    
    def get_db_error_msg(self):
        return self._db_error_collection.get_error_msg()
    
    def get_validation_errors_statistics_msg(self):
        return self._validation_error_collection.get_error_count_msg()
    
    def get_validation_errors_statistics_summary(self):
        return self._validation_error_collection.get_error_count_summary()

    def get_consistency_errors_statistics_msg(self):
        return self._consistency_error_collection.get_error_count_msg()

    def get_database_errors_statistics_msg(self):
        return self._db_error_collection.get_error_count_msg()

    def info(self, text):
        self._logger.info(text)

    # Methods affecting counters
    def increment_updated(self, stage):
        """Method incrementing updated records counter.
        """
        if not self._r_counter.has_key(stage):
            self._r_counter[stage] = RecordCounter() 
        self._r_counter[stage].increment('updated')

    def increment_inserted(self, stage):
        """Method incrementing inserted records counter.
        """
        if not self._r_counter.has_key(stage):
            self._r_counter[stage] = RecordCounter() 
        self._r_counter[stage].increment('inserted')

    def set_lab_db_count(self, stage, value):
        """Method setting number of records in lab_db for certain stage
        """
        if not self._r_counter.has_key(stage):
            self._r_counter[stage] = RecordCounter() 
        self._r_counter[stage]['lab_db'] = value

    def set_center_db_count(self, stage, value):
        """Method setting number of records in center_db for certain stage
        """
        if not self._r_counter.has_key(stage):
            self._r_counter[stage] = RecordCounter() 
        self._r_counter[stage]['center_db'] = value

    def get_count_values_msg(self):
        """Method returning counter values
        """
        message = ''
        for stage, rc in self._r_counter.items():
             message += '%s:\n'%(stage)
             for k, v in rc.items():
                message += '\t%s records: %s\n'%(k, v)
        return message

def ErrorHandlerSingleton():
    """Method returning the instance of the ErrorHandler class.
    """
    if not ErrorHandler._instance:
        ErrorHandler._instance = ErrorHandler()
    return ErrorHandler._instance


def main():
"""Main function testing functionality of the module.
"""
    a = ErrorHandlerSingleton()
    clone = 'clone'
    expression = 'expression'
    data_dict = {
        'experiment_date_start': '2010-05-10', 
        'lab_id': 'UVA', 
        'updated': '2011-06-30 14:11:00', 
        'vector': 'pET15b_HisTEV', 
        'local_protein_target_id': '992', 
        'pcr_primer_backward': 'AT CTC GAG TTA aca agc agg gcg cat cag', 
        'sequence': '', 
        'export_type': 'nysgrc', 
        'protocol_id': 'uva_cloning_1', 
        'protein_target_id': '490006', 
        'experiment_date_end': '2010-05-15', 
        'sequence_end': '164', 
        'clone_id': 'V29', 
        'local_clone_id': '29', 
        'status': 'success', 
        'pcr_primer_forward': 'GAA TTC CAT ATG aac ata gtt gat caa caa acc', 
        'export_isvalid': 'True', 
        'person_id': 'Aleksandra Knapik', 
        'dna_sequence': '', 
        'sequence_start': '1', 
        'clone_suffix': '29'
    }
    a.attach_validation_error('clone', 'protocol_id', data_dict, 'Protocol is gone....')
    data_dict = {
        'experiment_date_start': '2010-05-10', 
        'lab_id': 'UVA', 
        'updated': '2011-06-30 14:11:00', 
        'vector': 'pET15b_HisTEV', 
        'local_protein_target_id': '992', 
        'pcr_primer_backward': 'AT CTC GAG TTA aca agc agg gcg cat cag', 
        'sequence': '', 
        'export_type': 'nysgrc', 
        'protocol_id': 'uva_cloning_1', 
        'protein_target_id': '490007', 
        'experiment_date_end': '2010-05-15', 
        'sequence_end': '164', 
        'clone_id': 'V29', 
        'local_clone_id': '29', 
        'status': 'success', 
        'pcr_primer_forward': 'GAA TTC CAT ATG aac ata gtt gat caa caa acc', 
        'export_isvalid': 'True', 
        'person_id': 'Aleksandra Knapik', 
        'dna_sequence': '', 
        'sequence_start': '1', 
        'expression_suffix': '29'
    }
    
    a.attach_validation_error('clone', 'protocol_id', data_dict, 'Protocol is gone')
    data_dict = {
        'experiment_date_start': '2010-05-10', 
        'lab_id': 'UVA', 
        'updated': '2011-06-30 14:11:00', 
        'vector': 'pET15b_HisTEV', 
        'local_protein_target_id': '992', 
        'pcr_primer_backward': 'AT CTC GAG TTA aca agc agg gcg cat cag', 
        'sequence': '', 
        'export_type': 'nysgrc', 
        'protocol_id': 'uva_cloning_12', 
        'protein_target_id': '490007', 
        'experiment_date_end': '2010-05-15', 
        'sequence_end': '164', 
        'expression_id': 'V13', 
        'local_clone_id': '29', 
        'status': 'success', 
        'pcr_primer_forward': 'GAA TTC CAT ATG aac ata gtt gat caa caa acc', 
        'export_isvalid': 'True', 
        'person_id': 'Aleksandra Knapik', 
        'dna_sequence': '', 
        'sequence_start': '1', 
        'clone_suffix': '29'
    }
    a.attach_validation_error('expression', 'protocol_id', data_dict, 'Protocol is missing')
    a.attach_validation_error('expression', 'protocol_id', data_dict, 'Protocol is gone')
    a.attach_db_error('This is somekind of stacktrace with query and caught database exception content.')
    a.set_lab_db_count('clone', 231)
    a.set_center_db_count('expression', 232)
    a.increment_inserted('clone')
    a.increment_inserted('clone')
    a.increment_updated('clone')
    a.increment_updated('expression')

    print a.get_validation_error_msg()
    print a.get_db_error_msg()
    print 
    print a.get_count_values_msg()

if __name__ == '__main__':
    main()
