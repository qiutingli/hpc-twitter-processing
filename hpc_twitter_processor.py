from mpi4py import MPI
import json
from collections import defaultdict, Counter
import datetime
import logging
import re


def setup_logger():
    """
    Setup a logger for the application
    """
    global logger
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)


def start_timer():
    global start
    start = datetime.datetime.now()
    logger.info('The Application Started at: {}'.format(start))


def end_timer():
    end = datetime.datetime.now()
    logger.info('The Application Ended at: {}'.format(end))
    logger.info('The Application Total Execution Time: {} \n'.format((end-start)))


def parse_line(line):
    """
    Edit the line to make it can be parsed as JSON
    :param line:
    :return:
    """
    if line[-2] == ',':
        line_json = json.loads(line[:-2])
    else:
        line_json = json.loads(line[:-1])
    return line_json


def parse_json_file(json_filename):
    """
    Iteratively parse JSON
    :param json_filename:
    :param rank: the rank of current process
    :param size: the number of tasks
    :return: Hashtag set ans language set in the JSON file
    """
    hashtag_dict = defaultdict(int)
    language_dict = defaultdict(int)
    with open(json_filename, 'r') as input_file:
        for index, line in enumerate(input_file):
            try:
                if index % comm_size == comm_rank:
                    line_json = parse_line(line)
                    # update language dictionary
                    language_code = line_json['doc']['metadata']['iso_language_code']
                    language_dict[language_code] += 1
                    # update hashtag dictionary
                    text = line_json['doc']['text']
                    for word in text.split():
                        if word.startswith("#"):
                            word = word.lower()
                            try:
                                word = re.findall(r"[\w']+|[.,!?;]", word)[0]
                            except:
                                # exception in this case is that the word is '#'
                                pass
                            hashtag_dict[word] += 1
            except Exception as e:

                if "Expecting value: line 1" in str(e):
                    pass
                else:
                    logger.exception(e)

    return Counter(hashtag_dict), Counter(language_dict)


def display_results(hashtag_dict_list, language_code_dict_list):
    """
    Display the top 10 hashtags and tweetlanguages
    :param top10_hashtags: a counter with elements are top10 hashtags
    :param top10_language_codes: a counter with elements are top10 languages
    """
    language_code = ['am', 'ar', 'hy', 'bn', 'bg', 'my', 'zh', 'cs', 'da', 'nl', 'en',
                     'et', 'fi', 'fr', 'ka', 'de', 'el', 'gu', 'ht', 'iw', 'hi', 'hu',
                     'is', 'in', 'it', 'ja', 'kn', 'km', 'ko', 'lo', 'lv', 'lt', 'ml',
                     'dv', 'mr', 'ne', 'no', 'or', 'pa', 'ps', 'fa', 'pl', 'pt',
                     'ro', 'ru', 'sr', 'sd', 'si', 'sk', 'sl', 'ckb', 'es', 'sv', 'tl',
                     'ta', 'te', 'th', 'bo', 'tr', 'uk', 'ur', 'ug', 'vi', 'cy', 'und']

    language_name = ['Amharic', 'Arabic', 'Armenian', 'Bengali', 'Bulgarian', 'Burmese',
                     'Chinese', 'Czech', 'Danish', 'Dutch', 'English', 'Estonian',
                     'Finnish', 'French', 'Georgian', 'German', 'Greek', 'Gujarati',
                     'Haitian', 'Hebrew', 'Hindi', 'Hungarian', 'Icelandic', 'Indonesian',
                     'Italian', 'Japanese', 'Kannada', 'Khmer', 'Korean', 'Lao',
                     'Latvian', 'Lithuanian', 'Malayalam', 'Maldivian', 'Marathi', 'Nepali',
                     'Norwegian', 'Oriya', 'Panjabi', 'Pashto', 'Persian', 'Polish',
                     'Portuguese', 'Romanian', 'Russian', 'Serbian', 'Sindhi', 'Sinhala',
                     'Slovak', 'Slovenian', 'Sorani Kurdish', 'Spanish', 'Swedish', 'Tagalog',
                     'Tami', 'Telugu', 'Thai', 'Tibetan', 'Turkish', 'Ukrainian', 'Urdu',
                     'Uyghur', 'Vietnamese', 'Welsh', 'Undefined']

    # Merge dictionaries with summing up values of common keys (hashtags/languages)
    final_hashtag_set = sum(hashtag_dict_list, Counter())
    top10_hashtags = final_hashtag_set.most_common(10)
    final_language_code_set = sum(language_code_dict_list, Counter())
    top10_language_codes = final_language_code_set.most_common(10)

    print('Top 10 Hashtags Are: ')
    for index, tuple in enumerate(top10_hashtags):
        print('{}. #{}, {}'.format(index+1, tuple[0], tuple[1]))

    print('\n')
    print('Top 10 Languages Used Are: ')
    for index, tuple in enumerate(top10_language_codes):
        print('{}. {}({}), {}'.format(index+1, language_name[language_code.index(tuple[0])], tuple[0], tuple[1]))


def main():
    comm = MPI.COMM_WORLD
    global comm_rank, comm_size
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()
    if comm_rank == 0:
        start_timer()

    hashtag_dict, language_dict = parse_json_file('tinyTwitter.json')
    # Make sure all processes have reached this point
    comm.Barrier()
    # Gather results
    hashtag_dict_list = comm.gather(hashtag_dict, root=0)
    language_code_dict_list = comm.gather(language_dict, root=0)

    if comm_rank == 0:
        # Print out the results
        display_results(hashtag_dict_list, language_code_dict_list)
        end_timer()


if __name__ == '__main__':
    setup_logger()
    main()