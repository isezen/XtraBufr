# Copyright 2005-2017 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

#
# Python implementation: bufr_read_synop
#
# Description: how to read data values from BUFR messages.
#

# Please note that SYNOP reports can be encoded in various ways in BUFR.
# Therefore the code below might not work directly for other types of SYNOP
# messages than the one used in the example. It is advised to use bufr_dump to
# understand the structure of the messages.

from __future__ import print_function
import traceback
import sys

from eccodes import *

# INPUT = 'mss_0_2_20180324.bufr4'
INPUT = 'synop_multi_subset.bufr'
VERBOSE = 1  # verbose error reporting


def get_keys(bufr):
    keys = []
    iterid = codes_bufr_keys_iterator_new(bufr)
    while codes_bufr_keys_iterator_next(iterid):
        keyname = codes_bufr_keys_iterator_get_name(iterid)
        keys.append(keyname)
    codes_bufr_keys_iterator_delete(iterid)
    return(keys)


def extract_subset(bufr, subset_number):
    codes_set(bufr, 'extractSubset', subset_number)
    codes_set(bufr, 'doExtractSubsets', 1)
    bufr2 = codes_clone(bufr)
    codes_set(bufr2, 'unpack', 1)
    return(bufr2)


def get_subset(bufr, subset_number):
    extract_subset(bufr, subset_number)


def example():

    # define the keys to be printed
    keys1 = [
        'edition',
        'masterTableNumber',
        'bufrHeaderCentre',
        'bufrHeaderSubCentre',
        'updateSequenceNumber',
        'dataCategory',
        'internationalDataSubCategory',
        'dataSubCategory',
        'masterTablesVersionNumber',
        'localTablesVersionNumber',
        'typicalYear',
        'typicalMonth',
        'typicalDay',
        'typicalHour',
        'typicalMinute',
        'typicalSecond',
        'numberOfSubsets',
        'observedData',
        'compressedData']

    keys2 = [
        'blockNumber',
        'stationNumber',
        'stationOrSiteName',
        'latitude',
        'longitude',
        'year',
        'month',
        'day',
        'hour',
        'minute',
        'heightOfStationGroundAboveMeanSeaLevel',
        'heightOfBarometerAboveMeanSeaLevel',
        'nonCoordinatePressure',
        'pressureReducedToMeanSeaLevel',
        '3HourPressureChange',
        'characteristicOfPressureTendency',
        '24HourPressureChange',
        'pressure',
        'horizontalVisibility',
        'cloudCoverTotal',
        'cloudAmount',
        'heightOfBaseOfCloud',
        'cloudType']

    #     'airTemperature',
    #     'dewpointTemperature',
    #     'horizontalVisibility',
    #     'totalPrecipitationPast24Hours',
    #     'presentWeather',
    #     'pastWeather1',
    #     'pastWeather2',
    #     'windSpeed',
    #     'windDirection',
    #     'cloudCoverTotal',
    #     'cloudAmount',  # cloud amount (low and mid level)
    #     'heightOfBaseOfCloud',
    #     '#1#cloudType',  # cloud type (low clouds)
    #     '#2#cloudType',  # cloud type (middle clouds)
    #     '#3#cloudType'  # cloud type (highclouds)
    # ]

    # The cloud information is stored in several blocks in the
    # SYNOP message and the same key means a different thing in different
    # parts of the message. In this example we will read the first
    # cloud block introduced by the key
    # verticalSignificanceSurfaceObservations=1.
    # We know that this is the first occurrence of the keys we want to
    # read so in the list above we used the # (occurrence) operator
    # accordingly.

    # open bufr file
    f = open(INPUT)
    # loop for the messages in the file
    cnt = 0
    while 1:
        # get handle for message
        bufr = codes_bufr_new_from_file(f)
        if bufr is None:
            break

        print("message: %s" % cnt)
        cnt += 1

        # we need to instruct ecCodes to expand all the descriptors
        # i.e. unpack the data values
        nos = codes_get(bufr, 'numberOfSubsets')
        # if nos == 2:
        #     break

        # print the values for the selected keys from the message
        for key in keys1:
            try:
                print('  %s: %s' % (key, codes_get(bufr, key)))
            except CodesInternalError as e:
                print('Error with key="%s" : %s' % (key, e.msg),
                      file=sys.stderr)

        # try:
        #     codes_set(bufr, 'unpack', 1)
        # except DecodingError as e:
        #     print('%s' % e.msg, file=sys.stderr)
        codes_set(bufr, 'unpack', 1)
        print('')
        for i in range(1, nos + 1):
            bufr2 = extract_subset(bufr, i)
            keys2 = get_keys(bufr2)
            keys2 = keys2[20:]

            for key in keys2:
                try:
                    print('    {}:{}'.format(key, codes_get(bufr, key)))
                except CodesInternalError as e:
                    print('Error with key="%s" : %s' % (key, e.msg),
                          file=sys.stderr)
            print('')
            codes_release(bufr2)

        # delete handle
        codes_release(bufr)

    # close the file
    f.close()


def main():
    try:
        example()
    except CodesInternalError as err:
        if VERBOSE:
            traceback.print_exc(file=sys.stderr)
        else:
            sys.stderr.write(err.msg + '\n')

        return 1


if __name__ == "__main__":
    sys.exit(main())
