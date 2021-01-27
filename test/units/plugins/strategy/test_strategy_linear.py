# Copyright (c) 2018 Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

# Make coding more python3-ish
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

import json
from datetime import datetime

from units.compat import unittest
from units.compat.mock import patch, MagicMock

from ansible.executor.play_iterator import PlayIterator
from ansible.playbook import Playbook
from ansible.playbook.play_context import PlayContext
from ansible.plugins.action.command import ActionModule as CommandAction
from ansible.plugins.strategy.linear import StrategyModule
from ansible.executor.task_queue_manager import TaskQueueManager
from ansible.inventory.manager import InventoryManager

from units.mock.loader import DictDataLoader
from units.mock.path import mock_unfrackpath_noop


def mock_command_run(*args, **kwargs):
    """Mock successful execution of /bin/true"""
    result = {
        'cmd': '/bin/true',
        'stdout': '',
        'stderr': '',
        'rc': 0,
        'start': datetime.now().isoformat(),
        'end': datetime.now().isoformat(),
        'delta': '0:00:00.000010',
        'changed': True,
        'invocation': {'module_args': {
            '_raw_params': '/bin/true',
            '_uses_shell': True,
            'warn': True,
            'stdin_add_newline': True,
            'strip_empty_ends': True,
            'argv': None,
            'chdir': None,
            'executable': None,
            'creates': None,
            'removes': None,
            'stdin': None
        }},
        '_ansible_parsed': True,
        'stdout_lines': [],
        'stderr_lines': []
    }
    return result


class TestStrategyLinear(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('ansible.playbook.role.definition.unfrackpath', mock_unfrackpath_noop)
    def test_noop(self):
        fake_loader = DictDataLoader({
            "test_play.yml": """
            - hosts: all
              gather_facts: no
              tasks:
                - block:
                   - block:
                     - name: task1
                       debug: msg='task1'
                       failed_when: inventory_hostname == 'host01'

                     - name: task2
                       debug: msg='task2'

                     rescue:
                       - name: rescue1
                         debug: msg='rescue1'

                       - name: rescue2
                         debug: msg='rescue2'
            """,
        })

        mock_var_manager = MagicMock()
        mock_var_manager._fact_cache = dict()
        mock_var_manager.get_vars.return_value = dict()

        p = Playbook.load('test_play.yml', loader=fake_loader, variable_manager=mock_var_manager)

        hosts = []
        for i in range(0, 2):
            host = MagicMock()
            host.name = host.get_name.return_value = 'host%02d' % i
            hosts.append(host)

        mock_var_manager._fact_cache['host00'] = dict()

        inventory = MagicMock()
        inventory.get_hosts.return_value = hosts
        inventory.filter_hosts.return_value = hosts

        play_context = PlayContext(play=p._entries[0])

        itr = PlayIterator(
            inventory=inventory,
            play=p._entries[0],
            play_context=play_context,
            variable_manager=mock_var_manager,
            all_vars=dict(),
        )

        tqm = TaskQueueManager(
            inventory=inventory,
            variable_manager=mock_var_manager,
            loader=fake_loader,
            passwords=None,
            forks=5,
        )
        tqm._initialize_processes(3)
        strategy = StrategyModule(tqm)

        # implicit meta: flush_handlers
        hosts_left = strategy.get_hosts_left(itr)
        hosts_tasks = strategy._get_next_task_lockstep(hosts_left, itr)
        host1_task = hosts_tasks[0][1]
        host2_task = hosts_tasks[1][1]
        self.assertIsNotNone(host1_task)
        self.assertIsNotNone(host2_task)
        self.assertEqual(host1_task.action, 'meta')
        self.assertEqual(host2_task.action, 'meta')

        # debug: task1, debug: task1
        hosts_left = strategy.get_hosts_left(itr)
        hosts_tasks = strategy._get_next_task_lockstep(hosts_left, itr)
        host1_task = hosts_tasks[0][1]
        host2_task = hosts_tasks[1][1]
        self.assertIsNotNone(host1_task)
        self.assertIsNotNone(host2_task)
        self.assertEqual(host1_task.action, 'debug')
        self.assertEqual(host2_task.action, 'debug')
        self.assertEqual(host1_task.name, 'task1')
        self.assertEqual(host2_task.name, 'task1')

        # mark the second host failed
        itr.mark_host_failed(hosts[1])

        # debug: task2, meta: noop
        hosts_left = strategy.get_hosts_left(itr)
        hosts_tasks = strategy._get_next_task_lockstep(hosts_left, itr)
        host1_task = hosts_tasks[0][1]
        host2_task = hosts_tasks[1][1]
        self.assertIsNotNone(host1_task)
        self.assertIsNotNone(host2_task)
        self.assertEqual(host1_task.action, 'debug')
        self.assertEqual(host2_task.action, 'meta')
        self.assertEqual(host1_task.name, 'task2')
        self.assertEqual(host2_task.name, '')

        # meta: noop, debug: rescue1
        hosts_left = strategy.get_hosts_left(itr)
        hosts_tasks = strategy._get_next_task_lockstep(hosts_left, itr)
        host1_task = hosts_tasks[0][1]
        host2_task = hosts_tasks[1][1]
        self.assertIsNotNone(host1_task)
        self.assertIsNotNone(host2_task)
        self.assertEqual(host1_task.action, 'meta')
        self.assertEqual(host2_task.action, 'debug')
        self.assertEqual(host1_task.name, '')
        self.assertEqual(host2_task.name, 'rescue1')

        # meta: noop, debug: rescue2
        hosts_left = strategy.get_hosts_left(itr)
        hosts_tasks = strategy._get_next_task_lockstep(hosts_left, itr)
        host1_task = hosts_tasks[0][1]
        host2_task = hosts_tasks[1][1]
        self.assertIsNotNone(host1_task)
        self.assertIsNotNone(host2_task)
        self.assertEqual(host1_task.action, 'meta')
        self.assertEqual(host2_task.action, 'debug')
        self.assertEqual(host1_task.name, '')
        self.assertEqual(host2_task.name, 'rescue2')

        # implicit meta: flush_handlers
        hosts_left = strategy.get_hosts_left(itr)
        hosts_tasks = strategy._get_next_task_lockstep(hosts_left, itr)
        host1_task = hosts_tasks[0][1]
        host2_task = hosts_tasks[1][1]
        self.assertIsNotNone(host1_task)
        self.assertIsNotNone(host2_task)
        self.assertEqual(host1_task.action, 'meta')
        self.assertEqual(host2_task.action, 'meta')

        # implicit meta: flush_handlers
        hosts_left = strategy.get_hosts_left(itr)
        hosts_tasks = strategy._get_next_task_lockstep(hosts_left, itr)
        host1_task = hosts_tasks[0][1]
        host2_task = hosts_tasks[1][1]
        self.assertIsNotNone(host1_task)
        self.assertIsNotNone(host2_task)
        self.assertEqual(host1_task.action, 'meta')
        self.assertEqual(host2_task.action, 'meta')

        # end of iteration
        hosts_left = strategy.get_hosts_left(itr)
        hosts_tasks = strategy._get_next_task_lockstep(hosts_left, itr)
        host1_task = hosts_tasks[0][1]
        host2_task = hosts_tasks[1][1]
        self.assertIsNone(host1_task)
        self.assertIsNone(host2_task)

    @patch('ansible.inventory.manager.unfrackpath', mock_unfrackpath_noop)
    @patch('os.path.exists', lambda x: True)
    @patch('os.access', lambda x, y: True)
    @patch.object(CommandAction, 'run', mock_command_run)
    def test_ignore_max_fail_percentage(self):
        test_inventory = json.dumps({
            'test': {
                'hosts': {
                    'test_host_1': {},
                    'test_host_2': {}
                }
            }
        })
        fake_loader = DictDataLoader({
            "test_play.yml": """
                - name: FAILING PLAY
                  hosts: test
                  gather_facts: no
                  any_errors_fatal: true
                  tasks:
                    - name: successful-task
                      command: /bin/true
                      notify: failing-handler
                  handlers:
                    - name: failing-handler
                      fail:
                      when: ansible_host == 'test_host_2'
    
                - name: POST-FAIL PLAY
                  hosts: test
                  gather_facts: no
                  tasks:
                    - name: 'post-fail play'
                      debug: msg='UNEXPECTED PLAY RUN'
            """,
            "test_inventory.json": test_inventory
        })

        mock_var_manager = MagicMock()
        mock_var_manager._fact_cache = dict()
        mock_var_manager.get_vars.return_value = dict()

        host_names = []
        inventory = InventoryManager(loader=fake_loader, sources="test_inventory.json")
        for host in inventory.get_hosts(pattern='test'):
            host_names.append(host.name)
            mock_var_manager._fact_cache[host.name] = dict()

        tqm = TaskQueueManager(
            inventory=inventory,
            variable_manager=mock_var_manager,
            loader=fake_loader,
            passwords=None,
            forks=3,
        )
        tqm._initialize_processes(3)

        strategy = StrategyModule(tqm)
        strategy._hosts_cache = host_names
        strategy._hosts_cache_all = host_names

        play_book = Playbook.load('test_play.yml', loader=fake_loader, variable_manager=mock_var_manager)
        for play in play_book.get_plays():
            play_context = PlayContext(play=play)
            play_iterator = PlayIterator(
                inventory=inventory,
                play=play,
                play_context=play_context,
                variable_manager=mock_var_manager,
                all_vars=dict(),
            )
            play_return_code = strategy.run(play_iterator, play_context)
            # handler fails only once, but both hosts are marked failed
            # next play runs with empty host list (all were failed before)
            self.assertDictEqual(tqm._failed_hosts, {'test_host_2': True, 'test_host_1': True})
            self.assertEqual(play_return_code, tqm.RUN_FAILED_BREAK_PLAY)
