import salt.client
import time
from collections import defaultdict
'''
Примеры использования:
salt-run minion_cron.update_cron minion1
salt-run minion_cron.minute_order web*
salt-run minion_cron.stats
'''
def update_cron(minion, minute, user='root'):
    '''
    Обновление задания одного миньона
    Принимает:
      minion: имя миньона
      minute: минуты
      user: пользователь
    '''
    info = {}
    #print(minion, minute)
    client = salt.client.LocalClient(__opts__['conf_file'])
    info[minion] = client.cmd(minion, 'cron.set_job',
                           kwarg={'user': user,
                                  'minute': minute,
                                  'hour': '*',
                                  'daymonth': '*',
                                  'month': '*',
                                  'dayweek': '*',
                                  'cmd': '/usr/local/sbin/minion_apply.py',
                                  'commented': False,
                                  'identifier': 'salt_apply'
                                  }, timeout=1)
    return


def minute_order(tgt='*', user='root'):
    '''
    Цель: равномерное распределение заданий в течении часа
    ВНИМАНИЕ! Если задание закоментарено, то миньон пропускается

    Получает:
      tgt: маску миньонов
      user: пользователя
    '''
    info = {}
    minute = 0
    client = salt.client.LocalClient(__opts__['conf_file'])
    minions = client.cmd(tgt, 'cron.list_tab', [user], timeout=1)
    for minion in sorted(minions):
        try:
            for cron in minions[minion]['crons']:
                # проверяем, что задание действительно есть и не закоментарено
                if cron['identifier'] == 'salt_apply':
                    info[minion] = minute
                    update_cron(minion, minute)
                    if minute == 59:
                        minute = 0
                    else:
                        minute += 1
        except Exception as e:
            print(minute, minion, e)
            continue
    return info


def stats(tgt='*', user='root'):
    '''
    Показывает статистику распределения заданий по времени
    в отсортированном виде.

    Пример:
    |_
      - 01_1       01 - номер по популярности, 1 - минута
      |_
        - api1
        - sd-100
        - sd-40
        - ds-96
        - sb4
        - storage9
    |_
      - 02_4
      |_
        - pyw1
        - kds-103
        - kds-43
        - kds-99
        - haes1
        - testrail1
    |_

    '''
    info = {}
    minute = 0
    client = salt.client.LocalClient(__opts__['conf_file'])
    minions = client.cmd(tgt, 'cron.list_tab', [user], timeout=1)
    for minion in sorted(minions):
        try:
            for cron in minions[minion]['crons']:
                # проверяем, что задание действительно есть и не закоментарено
                if cron['identifier'] == 'salt_apply':
                    info[minion] = cron['minute']
        except Exception as e:
            print(minion, e)

    # TODO сделай лучше
    # Группируем по минутам
    # https://stackoverflow.com/questions/15751979/grouping-python-dictionary-keys-as-a-list-and-create-a-new-dictionary-with-this
    grouped_by_minute = defaultdict(list)
    for key, value in sorted(info.items()):
        grouped_by_minute[value].append(key)

    # Сортируем по колличеству миньонов в минуту
    i = 0
    for m in sorted(grouped_by_minute, key=lambda m: len(grouped_by_minute[m]), reverse=True):
        i += 1
        if i < 10:
            n = '0' + str(i)
        else:
            n = str(i)
        grouped_by_minute[n + '_' + str(m) ] = grouped_by_minute.pop(m)
    return sorted(grouped_by_minute.items())

