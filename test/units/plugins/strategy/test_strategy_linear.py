# Copyright (c) 2018 Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

# Make coding more python3-ish
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

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
from ansible.vars.manager import VariableManager

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
        fake_loader = DictDataLoader({
            "playbook.yml": failing_handler_yml,
            "inventory.yml": inventory_yml,
        })
        inventory = InventoryManager(loader=fake_loader, sources="inventory.yml")
        var_manager = VariableManager(loader=fake_loader, inventory=inventory)
        tqm = TaskQueueManager(
            inventory=inventory,
            variable_manager=var_manager,
            loader=fake_loader,
            passwords=None,
            forks=3,
        )
        play_book = Playbook.load(
            'playbook.yml',
            loader=fake_loader,
            variable_manager=var_manager
        )
        for play in play_book.get_plays():
            play_return_code = tqm.run(play)
            # handler fails only once, but both hosts are marked failed
            # next play runs with empty host list (all were failed before)
            self.assertEqual(play_return_code, tqm.RUN_FAILED_BREAK_PLAY)
            self.assertDictEqual(tqm._failed_hosts, {'test_host_2': True, 'test_host_1': True})

    @patch('ansible.inventory.manager.unfrackpath', mock_unfrackpath_noop)
    @patch('os.path.exists', lambda x: True)
    @patch('os.access', lambda x, y: True)
    def test_max_fail_percentage(self):
        fake_loader = DictDataLoader({
            "playbook.yml": fail_percentage_60_yml,
            "inventory.yml": inventory_yml,
        })
        inventory = InventoryManager(loader=fake_loader, sources="inventory.yml")
        var_manager = VariableManager(loader=fake_loader, inventory=inventory)
        tqm = TaskQueueManager(
            inventory=inventory,
            variable_manager=var_manager,
            loader=fake_loader,
            passwords=None,
            forks=3,
        )
        play_book = Playbook.load(
            'playbook.yml',
            loader=fake_loader,
            variable_manager=var_manager
        )
        for play in play_book.get_plays():
            play_return_code = tqm.run(play)
            # handler fails only once, but both hosts are marked failed
            # next play runs with empty host list (all were failed before)
            self.assertEqual(play_return_code, tqm.RUN_FAILED_HOSTS)
            self.assertDictEqual(tqm._failed_hosts, {'test_host_1': True})

    @patch('ansible.inventory.manager.unfrackpath', mock_unfrackpath_noop)
    @patch('os.path.exists', lambda x: True)
    @patch('os.access', lambda x, y: True)
    def test_max_fail_percentage_reached(self):
        fake_loader = DictDataLoader({
            "playbook.yml": fail_percentage_40_yml,
            "inventory.yml": inventory_yml,
        })
        inventory = InventoryManager(loader=fake_loader, sources="inventory.yml")
        var_manager = VariableManager(loader=fake_loader, inventory=inventory)
        tqm = TaskQueueManager(
            inventory=inventory,
            variable_manager=var_manager,
            loader=fake_loader,
            passwords=None,
            forks=3,
        )
        play_book = Playbook.load(
            'playbook.yml',
            loader=fake_loader,
            variable_manager=var_manager
        )
        for play in play_book.get_plays():
            play_return_code = tqm.run(play)
            self.assertEqual(play_return_code, tqm.RUN_FAILED_BREAK_PLAY)
            self.assertDictEqual(tqm._failed_hosts, {'test_host_2': True, 'test_host_1': True})

    @patch('ansible.inventory.manager.unfrackpath', mock_unfrackpath_noop)
    @patch('os.path.exists', lambda x: True)
    @patch('os.access', lambda x, y: True)
    def test_last_task_failed_in_block_with_always(self):
        """
        Mostly the same as TestPlayIterator.test_failed_block_with_always
        in units/executor/test_play_iterator.py, but failing task is last in block
        """
        fake_loader = DictDataLoader({
            "playbook.yml": last_failing_in_block_with_always,
            "inventory.yml": inventory_yml,
        })
        inventory = InventoryManager(loader=fake_loader, sources="inventory.yml")
        var_manager = VariableManager(loader=fake_loader, inventory=inventory)
        tqm = TaskQueueManager(
            inventory=inventory,
            variable_manager=var_manager,
            loader=fake_loader,
            passwords=None,
            forks=3,
        )
        play_book = Playbook.load(
            'playbook.yml',
            loader=fake_loader,
            variable_manager=var_manager
        )
        for play in play_book.get_plays():
            play_return_code = tqm.run(play)
            self.assertEqual(play_return_code, tqm.RUN_FAILED_BREAK_PLAY)
            self.assertDictEqual(tqm._failed_hosts, {'test_host_2': True, 'test_host_1': True})
            break


inventory_yml = '''
---
all:
  hosts:
    test_host_1:
      ansible_connection: local
    test_host_2:
      ansible_connection: local
'''
failing_handler_yml = """
---
- name: FAILING PLAY
  hosts: all
  gather_facts: no
  any_errors_fatal: true
  tasks:
    - name: SUCCESSFUL TASK
      command: /bin/true
      notify: FAILING HANDLER
  handlers:
    - name: FAILING HANDLER
      fail:
      when: ansible_host == 'test_host_2'
"""
last_failing_in_block_with_always = '''
---
- name: BLOCKED PLAY
  hosts: all
  gather_facts: no
  any_errors_fatal: false
  tasks:
    - block:
      - debug: msg='first in block'
      - name: FAILING TASK
        fail:
        when: ansible_host == 'test_host_2'
      always:
      - name: ALWAYS 
        debug: msg='in always'      
      any_errors_fatal: true
    - name: AFTER-BLOCK
      debug: msg='after-block task'
'''
fail_percentage_60_yml = '''
---
- name: SUCCESSFUL PLAY
  hosts: all
  gather_facts: no
  max_fail_percentage: 60
  tasks:
    - name: FLAKY TASK
      fail:
      when: ansible_host == 'test_host_1'
'''
fail_percentage_40_yml = '''
---
- name: FAILING PLAY
  hosts: all
  gather_facts: no
  max_fail_percentage: 40
  tasks:
    - name: FLAKY TASK
      fail:
      when: ansible_host == 'test_host_2'
'''
