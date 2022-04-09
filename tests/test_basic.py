import os
import unittest
from fshtbkvs.FSHTBKVS import FSHTBKVS

class TestFSHTBKVS(unittest.TestCase):
	# sha256sum of 'FSHTBKVS':
	# 60bffff92d13cbd3fe063c018a2263238b01459c388edd5a1bcef5e61d0c46b5
	def setUp(self):
		self.kvs_root 	= '/tmp'
		self.kvs_name 	= 'test_fshtbkvs'
		self.max_depth 	= 3
		self.kvs_path 	= os.path.join(self.kvs_root, self.kvs_name)

	def test_000_kvs_created(self):
		"""
		Test if kvs was created
		"""
		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name,
			max_depth=self.max_depth
		)

		self.assertTrue(
			os.path.exists(self.kvs_path),
			self.kvs_path + ' not found!'
		)

		meta_file = os.path.join(self.kvs_path, 'meta.json')
		self.assertTrue(
			os.path.exists(meta_file),
			meta_file + ' not found!'
		)

		chars = [c for c in '0123456789abcdef']
		for c0 in chars:
			for c1 in chars:
				for c2 in chars:
					f = os.path.join(
						self.kvs_path,
						c0,
						c1,
						c2 + '.json'
					)
					self.assertTrue(
						os.path.exists(f),
						f + ' not found!'
					)

	def test_001_meta_file_loading_and_getters(self):
		"""
		Test if value of a non existing key was written to file
		"""
		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		self.assertEqual(kvs.get_kvs_name(), self.kvs_name)
		self.assertEqual(kvs.get_max_depth(), self.max_depth)
		self.assertEqual(kvs.get_entries(), 0)

	def test_002_write_non_existing_key(self):
		"""
		Test if value of a non existing key was written to file
		"""
		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		self.assertEqual(kvs.write('FSHTBKVS', 'is awesome!'), 1)

		path_to_file = os.path.join(self.kvs_path, '6/0/b.json')
		with open(path_to_file, 'r') as f:
			data = f.read()
			f.close()

		data_expected = (
			'{"'
			+ '60bffff92d13cbd3fe063c018a2263238b01459c388edd5a1bcef5e61d0c46b5'
			+ '": "is awesome!"}'
		)

		self.assertEqual(data, data_expected)
		self.assertEqual(kvs.get_entries(), 1)

	def test_003_write_existing_key(self):
		"""
		Test if value of an existing key was updated
		"""
		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		self.assertEqual(kvs.write('FSHTBKVS', 'is really awesome!'), 1)

		path_to_file = os.path.join(self.kvs_path, '6/0/b.json')
		with open(path_to_file, 'r') as f:
			data = f.read()
			f.close()

		data_expected = (
			'{"'
			+ '60bffff92d13cbd3fe063c018a2263238b01459c388edd5a1bcef5e61d0c46b5'
			+ '": "is really awesome!"}'
		)

		self.assertEqual(data, data_expected)
		self.assertEqual(kvs.get_entries(), 1)

	def test_004_write_to_non_empty_json_file(self):
		"""
		Test if a new value can be added to non empty json file
		"""
		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		self.assertEqual(kvs.write('60bffff92d', 1337), 1)

		path_to_file = os.path.join(self.kvs_path, '6/0/b.json')
		with open(path_to_file, 'r') as f:
			data = f.read()
			f.close()

		data_expected = (
			'{"'
			+ '60bffff92d13cbd3fe063c018a2263238b01459c388edd5a1bcef5e61d0c46b5'
			+ '": "is really awesome!", "60bffff92d": 1337}'
		)

		self.assertEqual(data, data_expected)
		self.assertEqual(kvs.get_entries(), 2)

	def test_005_read_existing_key(self):
		"""
		Test if existing key can be read
		"""
		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		path_to_file = os.path.join(self.kvs_path, '6/0/b.json')
		with open(path_to_file, 'r') as f:
			data = f.read()
			f.close()

		self.assertEqual(kvs.read('FSHTBKVS'), 'is really awesome!')
		self.assertEqual(kvs.read('60bffff92d'), 1337)

	def test_006_read_non_existing_key(self):
		"""
		Test if existing key can be read
		"""
		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		self.assertEqual(kvs.read('1337'), None)

	def test_007_delete_existing_key(self):
		"""
		Test if existing key can be deleted
		"""
		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		self.assertEqual(kvs.read('60bffff92d'), 1337)
		self.assertEqual(kvs.delete('60bffff92d'), 1)
		self.assertEqual(kvs.read('60bffff92d'), None)
		self.assertEqual(kvs.get_entries(), 1)

	def test_008_delete_non_existing_key(self):
		"""
		Test if non existing key can be deleted without errors
		"""
		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		self.assertEqual(kvs.read('60bffff92d'), None)
		self.assertEqual(kvs.delete('60bffff92d'), 1)
		self.assertEqual(kvs.read('60bffff92d'), None)
		self.assertEqual(kvs.get_entries(), 1)

	def test_009_write_list(self):
		"""
		Test if a list can be stored in the kvs
		"""

		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		the_first_list = [
			'I like big butts and I can not lie',
			1584662760,
			False,
			3.141592
		]

		self.assertEqual(kvs.write('the_first_list', the_first_list), 1)
		self.assertEqual(kvs.read('the_first_list'), the_first_list)

		class ClassesAreNotSupported:
			def __init__(self, power=0):
				self.power = power
				if self.power > 9000:
					print("It's Over 9000!!!")

		piccolo = ClassesAreNotSupported(408)
		the_first_list.append(piccolo)

		with self.assertRaises(ValueError):
			kvs.write('the_first_list', the_first_list)

		self.assertEqual(kvs.delete('the_first_list'), 1)

	def test_010_write_nested_list(self):
		"""
		Test if a nested list can be stored in the kvs
		"""

		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		the_first_nested_list = [
			[
				1,
				'Django: The web framework for perfectionists with deadlines.'
			],
			[
				2,
				'Bottle: Python Web Framework'
			],
			[
				3,
				'Flask'
			]
		]

		self.assertEqual(kvs.write('the_first_nested_list', the_first_nested_list), 1)
		self.assertEqual(kvs.read('the_first_nested_list'), the_first_nested_list)

		class ClassesAreNotSupported:
			def __init__(self, power=0):
				self.power = power
				if self.power > 9000:
					print("It's Over 9000!!!")

		bulma = ClassesAreNotSupported(12)
		the_first_nested_list[-1].append(bulma)

		with self.assertRaises(ValueError):
			kvs.write('the_first_nested_list', the_first_nested_list)

		self.assertEqual(kvs.delete('the_first_nested_list'), 1)

	def test_011_write_dict(self):
		"""
		Test if a dict can be stored in the kvs
		"""

		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		the_first_dict = {
			'brand': 	'Ford',
			'model': 	'Mustang',
			'year': 	1964
		}

		self.assertEqual(kvs.write('the_first_dict', the_first_dict), 1)
		self.assertEqual(kvs.read('the_first_dict'), the_first_dict)

		the_first_dict[0] = 0
		with self.assertRaises(ValueError):
			kvs.write('the_first_dict', the_first_dict)
		del the_first_dict[0]

		class ClassesAreNotSupported:
			def __init__(self, power=0):
				self.power = power
				if self.power > 9000:
					print("It's Over 9000!!!")

		piccolo = ClassesAreNotSupported(408)
		the_first_dict['driver'] = piccolo

		with self.assertRaises(ValueError):
			kvs.write('the_first_dict', the_first_dict)

		self.assertEqual(kvs.delete('the_first_dict'), 1)

	def test_012_write_nested_dict(self):
		"""
		Test if a nested dict can be stored in the kvs
		"""

		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		the_first_nested_dict = {
			'0': {
				'brand': 	'Ford',
				'model': 	'Mustang',
				'year': 	1964
			},
			'1': {
				'brand': 	'AC Cars',
				'model': 	'AC Cobra',
				'year': 	1962
			},
			'2': {
				'brand': 	'Rimac',
				'model': 	'Nevera',
				'year': 	2021
			}
		}

		self.assertEqual(kvs.write('the_first_nested_dict', the_first_nested_dict), 1)
		self.assertEqual(kvs.read('the_first_nested_dict'), the_first_nested_dict)

		class ClassesAreNotSupported:
			def __init__(self, power=0):
				self.power = power
				if self.power > 9000:
					print("It's Over 9000!!!")

		bulma = ClassesAreNotSupported(12)
		the_first_nested_dict['2']['driver'] = bulma

		with self.assertRaises(ValueError):
			kvs.write('the_first_nested_dict', the_first_nested_dict)

		self.assertEqual(kvs.delete('the_first_nested_dict'), 1)

	def test_013_maintain_kvs_json_file_broken(self):
		"""
		Test if the kvs can get maintained when a json file is broken
		"""
		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		path_to_file = os.path.join(self.kvs_path, 'f/f/f.json')

		self.assertEqual(kvs.write('fffffff', "Epstein didn't kill himself"), 1)
		self.assertEqual(kvs.read('fffffff'), "Epstein didn't kill himself")
		self.assertEqual(kvs.get_entries(), 2)

		with open(path_to_file, 'w', encoding='UTF-8') as f:
			f.write('{"fffffff": "Epstein didn\'t kill himself"')
			f.close()

		self.assertEqual(kvs.read('fffffff'), None)
		self.assertEqual(kvs.get_entries(), 1)
		with open(path_to_file, 'r', encoding='UTF-8') as f:
			data = f.read()
			f.close()
		self.assertEqual(data, '{}')

	def test_014_maintain_kvs_json_file_lost(self):
		"""
		Test if the kvs can get maintained when a json file is lost
		"""
		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		self.assertEqual(kvs.write('fffffff', "Epstein didn't kill himself"), 1)
		self.assertEqual(kvs.read('fffffff'), "Epstein didn't kill himself")
		self.assertEqual(kvs.get_entries(), 2)

		path_to_file = os.path.join(self.kvs_path, 'f/f/f.json')
		os.remove(path_to_file)

		self.assertEqual(kvs.read('fffffff'), None)
		self.assertEqual(kvs.get_entries(), 1)
		with open(path_to_file, 'r', encoding='UTF-8') as f:
			data = f.read()
			f.close()
		self.assertEqual(data, '{}')

	def test_015_maintain_kvs_meta_file_broken(self):
		"""
		Test if the kvs can get maintained when the meta file is broken
		"""

		path_to_file = os.path.join(self.kvs_path, 'meta.json')
		with open(path_to_file, 'w', encoding='UTF-8') as f:
			f.write(
				'{"kvs_name": "test_fshtbkvs", "max_depth": 3, "entries": 1'
			)
			f.close()

		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		self.assertEqual(kvs.read('FSHTBKVS'), 'is really awesome!')

		path_to_file = os.path.join(self.kvs_path, 'meta.json')
		with open(path_to_file, 'r') as f:
			data = f.read()
			f.close()
		data_expected = (
			'{"kvs_name": "test_fshtbkvs", "max_depth": 3, "entries": 1}'
		)
		self.assertEqual(data, data_expected)

	def test_016_maintain_kvs_meta_file_lost(self):
		"""
		Test if the kvs can get maintained when the meta file is lost
		"""

		path_to_file = os.path.join(self.kvs_path, 'meta.json')
		os.remove(path_to_file)

		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		self.assertEqual(kvs.read('FSHTBKVS'), 'is really awesome!')

		path_to_file = os.path.join(self.kvs_path, 'meta.json')
		with open(path_to_file, 'r') as f:
			data = f.read()
			f.close()
		data_expected = (
			'{"kvs_name": "test_fshtbkvs", "max_depth": 3, "entries": 1}'
		)
		self.assertEqual(data, data_expected)

	def test_017_export_kvs(self):
		"""
		Test if the kvs can get exported
		"""

		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		entries = {
			"e94e8ed9c2": "alfredissimo!",
			"5db1fee4b5": 1337,
			"26436a96b6": "Who controls the past controls the future.",
			"58b6c6c96c": "https://blog.fefe.de/",
			"8139b33952": 0.9,
			"f99f33e288": "de_dust2",
			"e08e0bd862": "Who controls the present controls the past.",
			"3cbc87c768": True,
			"bbb9abde2d": "Wenn eine Aussage einen trifft, war sie wahrscheinlich auch zutreffend.",
			"de650d61f5": 4711
		}
		for key, value in entries.items():
			self.assertEqual(kvs.write(key, value), 1)

		path_to_file = os.path.join(self.kvs_path, 'd/e/6.json')
		with open(path_to_file, 'w', encoding='UTF-8') as f:
			f.write('{"de650d61f5": 4711')
			f.close()

		path_to_file = os.path.join(
			self.kvs_root,
			self.kvs_name,
			self.kvs_name + '.fshtbkvs'

		)
		kvs.export_kvs(str(path_to_file))

		expected_exports = [
			'{"26436a96b6": "Who controls the past controls the future."}',
			'{"3cbc87c768": true}',
			'{"58b6c6c96c": "https://blog.fefe.de/"}',
			'{"5db1fee4b5": 1337}',
			'{"60bffff92d13cbd3fe063c018a2263238b01459c388edd5a1bcef5e61d0c46b5": "is really awesome!"}',
			'{"8139b33952": 0.9}',
			'{"bbb9abde2d": "Wenn eine Aussage einen trifft, war sie wahrscheinlich auch zutreffend."}',
			'{"e08e0bd862": "Who controls the present controls the past."}',
			'{"e94e8ed9c2": "alfredissimo!"}',
			'{"f99f33e288": "de_dust2"}'
		]

		with open(path_to_file, 'r') as f:
			lines = f.readlines()
			f.close()

		line_number = 0
		for line in lines:
			data_expected = expected_exports[line_number]
			self.assertEqual(line.strip(), data_expected)
			line_number += 1

	def test_018_wipe_kvs(self):
		"""
		Test if the kvs can get wiped
		"""

		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		self.assertEqual(kvs.wipe_kvs(), 1)

		chars = [c for c in '0123456789abcdef']
		for c0 in chars:
			for c1 in chars:
				for c2 in chars:
					path_to_file = os.path.join(
						self.kvs_path,
						c0,
						c1,
						c2 + '.json'
					)

					with open(path_to_file, 'r') as f:
						data = f.read()
						f.close()

					self.assertEqual(data, '{}')

		self.assertEqual(kvs.get_entries(), 0)

	def test_019_import_kvs(self):
		"""
		Test if the kvs can get wiped
		"""

		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		path_to_file = os.path.join(
			self.kvs_root,
			self.kvs_name,
			self.kvs_name + '.fshtbkvs'

		)

		self.assertEqual(kvs.import_kvs(str(path_to_file)), 1)

		self.assertEqual(kvs.read('26436a96b6'), 'Who controls the past controls the future.')
		self.assertEqual(kvs.read('3cbc87c768'), True)
		self.assertEqual(kvs.read('58b6c6c96c'), 'https://blog.fefe.de/')
		self.assertEqual(kvs.read('5db1fee4b5'), 1337)
		self.assertEqual(kvs.read('FSHTBKVS'), 'is really awesome!')
		self.assertEqual(kvs.read('8139b33952'), 0.9)
		self.assertEqual(kvs.read('bbb9abde2d'), 'Wenn eine Aussage einen trifft, war sie wahrscheinlich auch zutreffend.')
		self.assertEqual(kvs.read('e08e0bd862'), 'Who controls the present controls the past.')
		self.assertEqual(kvs.read('e94e8ed9c2'), 'alfredissimo!')
		self.assertEqual(kvs.read('f99f33e288'), 'de_dust2')

		self.assertEqual(kvs.get_entries(), 10)

	def test_020_get_size_of_kvs(self):
		"""
		Test if the size in bytes of the kvs can get calculated
		"""

		kvs = FSHTBKVS(
			self.kvs_root,
			self.kvs_name
		)

		self.assertEqual(kvs.get_size_of_kvs(), 0.008627)

def purge_test_kvs():
	if os.path.exists('/tmp/test_fshtbkvs'):
		os.system('rm -rf /tmp/test_fshtbkvs')

if __name__ == '__main__':
	purge_test_kvs()
	unittest.main()
	# purge_test_kvs()
