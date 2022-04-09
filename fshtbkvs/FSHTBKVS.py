import hashlib
import json
import os
from pathlib import Path

class FSHTBKVS:
	"""
	Filesystem Hash Table Based Key Value Store (FSHTBKVS)

	The FSHTBKVS was developed to be an easy to deploy
	key value store with fast lookup times for (very)
	large data sets and without third party dependencies.

	max_depth and the amount of files created:
	1: 16^1 = 16
	2: 16^2 = 256
	3: 16^3 = 4.096
	4: 16^4 = 65.536
	5: 16^5 = 1.048.576
	6: 16^6 = 16.777.216
	7: 16^7 = 268.435.456
	"""
	def __init__(self, root_dir, kvs_name, max_depth=4):
		self.__root_dir  = os.path.normpath(root_dir)
		if not os.path.exists(root_dir):
			raise ValueError("root_dir '" + root_dir + "' does not exist")

		self.__kvs_name   = kvs_name
		self.__root_dir   = os.path.join(self.__root_dir, self.__kvs_name)
		self.__meta_file  = os.path.join(self.__root_dir, 'meta.json')
		self.__max_depth  = max_depth if max_depth in range(1, 7) else 4
		self.__entries    = 0
		self.__all_file_paths   = []
		self.__all_folder_paths = []

		if not os.path.exists(self.__root_dir):
			os.makedirs(self.__root_dir)
			self.__create_meta_file()
			self.__build_all_paths()

		if self.__load_meta_file() is False:
			raise OSError(
				"Not able to load or restore the meta file: "
				+ str(self.__meta_file)
			)

	def delete(self, key):
		"""
		Deletes an entry with the key 'key' from the kvs
		"""

		self.__validate_key(key)

		key  = self.__process_key(key)
		file = self.__get_file_by_key(key)

		data = self.__load_dict_from_json_file(file)

		if not key in data:
			return 1

		del data[key]
		data_written 	= self.__save_dict_to_json_file(file, data)
		if not data_written:
			return -1

		self.__entries -= 1
		self.__create_meta_file()

		return 1

	def export_kvs(self, file=''):
		"""
		Exports whole kvs data as importable .fshtbkvs file
		"""

		if file == '':
			file = os.path.join(
				os.path.dirname(self.__root_dir),
				self.__kvs_name + '.fshtbkvs'
			)

		# build all file paths
		if self.__all_file_paths == []:
			self.__build_all_file_paths()

		# write all key value pairs to export file
		try:
			with open(file, 'w') as f_export:
				for f in self.__all_file_paths:
					data = self.__load_dict_from_json_file(f)
					for key, value in data.items():
						f_export.write(
							json.dumps(
								{key: value},
								ensure_ascii=False
							)
							+ '\n'
						)
				f_export.close()
		except:
			raise OSError(
				"Not able to write kvs export to file: "
				+ str(file)
			)

		return 1

	def get_entries(self):
		return self.__entries

	def get_kvs_name(self):
		return self.__kvs_name

	def get_max_depth(self):
		return self.__max_depth

	def get_size_of_kvs(self):
		"""
		Returns the size of all json files used for storing data in megabytes
		"""

		kvs_size_in_megabytes = 0.0

		if self.__all_file_paths == []:
			self.__build_all_file_paths()

		for f in self.__all_file_paths:
			if not os.path.exists(f):
				self.maintain_kvs()
			kvs_size_in_megabytes += (
				os.path.getsize(f) / 1000 / 1000
			)

		return round(kvs_size_in_megabytes, 6)

	def import_kvs(self, file=''):
		"""
		Imports .fshtbkvs file into the kvs
		"""

		if file == '':
			raise ValueError("file must not be empty")
		if not os.path.exists(file):
			raise ValueError("file does not exist")

		# get all key value pairs line by line and add it to the kvs
		with open(file, 'r') as f:
			while True:
				line = f.readline()
				if not line:
					break
				if line.strip() == '':
					continue
				try:
					for key, value in json.loads(line.strip()).items():
						self.write(key, value)
				except:
					continue

		return 1

	def maintain_kvs(self):
		"""
		Rebuilds broken .json files and, creates missing files and folders
		and creates an updated meta file
		"""

		# build all paths
		self.__build_all_paths()

		# cleanup all json files
		self.__entries = 0
		for f in self.__all_file_paths:
			data 		= self.__load_dict_from_json_file(f)
			data_clean 	= {}

			for key, value in data.items():
				try:
					self.__validate_key(key)
					self.__validate_value(value)
					data_clean[key] = value
				except:
					continue

			data_written = self.__save_dict_to_json_file(f, data_clean)
			if not data_written:
				return -1

			self.__entries += len(data_clean)

		self.__create_meta_file()

		return 1

	def read(self, key):
		"""
		Returns the entry with the key 'key' from the kvs or 'None', if no entry
		exists
		"""

		self.__validate_key(key)

		key  = self.__process_key(key)
		file = self.__get_file_by_key(key)

		data = self.__load_dict_from_json_file(file)

		if not key in data:
			return None

		value = data[key]
		self.__validate_value(value)

		return value

	def wipe_kvs(self):
		"""
		Deletes every entry from the kvs and creates an updated meta file
		"""

		# delete meta file for auto maintainance, if wiping fails
		os.remove(self.__meta_file)

		# make sure, all files and folders exist
		self.__build_all_paths()

		# wipe all json files
		for f in self.__all_file_paths:
			self.__save_dict_to_json_file(f, {})

		self.__entries = 0

		return self.__create_meta_file()

	def write(self, key, value):
		"""
		Adds (or updates) an entry with the key 'key' and the value 'value'
		"""

		self.__validate_key(key)
		self.__validate_value(value)

		key  		= self.__process_key(key)
		file 		= self.__get_file_by_key(key)
		key_existed = False

		data = self.__load_dict_from_json_file(file)
		if key in data:
			key_existed = True

		data[key] 		= value
		data_written 	= self.__save_dict_to_json_file(file, data)
		if not data_written:
			return -1

		if not key_existed:
			self.__entries += 1
			self.__create_meta_file()

		return 1

	def __build_all_paths(self):
		"""
		Gathers all file/folder paths and creates those when missing
		"""

		# build all file paths
		if self.__all_file_paths == []:
			self.__build_all_file_paths()
		# build all folder paths
		if self.__all_folder_paths == []:
			self.__build_all_folder_paths()
		# create all folders
		for f in self.__all_folder_paths:
			os.makedirs(f, exist_ok=True)
		# create all files
		for f in self.__all_file_paths:
			if not os.path.exists(f):
				self.__save_dict_to_json_file(f, {})

	def __build_all_file_paths(self):
		"""
		Calculates all json file paths
		"""

        # build all file paths
		def build_all_file_paths(r_root_dir, r_current_depth=1):
			# build all file paths recursivly
			r_chars = [c for c in '0123456789abcdef']
			r_paths = []
			# base case: return with list of files
			if r_current_depth == self.__max_depth:
				return [
					(os.path.join(r_root_dir, c + '.json')) for c in r_chars
				]
			# recursion: go one step further in the filesystem
			for c in r_chars:
				for p in build_all_file_paths(
					os.path.join(r_root_dir, c),
					r_current_depth + 1
				):
					r_paths.append(p)
			return r_paths
		self.__all_file_paths = build_all_file_paths(self.__root_dir)

	def __build_all_folder_paths(self):
		"""
		Calculates all folder paths
		"""

		# iterate over all files an extract their folders
		for f in self.__all_file_paths:
			path_tmp = os.path.split(f)[0]
			while path_tmp != self.__root_dir:
				if not path_tmp in self.__all_folder_paths:
					self.__all_folder_paths.append(path_tmp)
				path_tmp = os.path.dirname(path_tmp)

		self.__all_folder_paths = sorted(self.__all_folder_paths)

	def __create_meta_file(self):
		"""
		Creates the kvs meta file
		"""

		meta = {
			'kvs_name':  self.__kvs_name,
			'max_depth': self.__max_depth,
			'entries':   self.__entries
		}
		return self.__save_dict_to_json_file(self.__meta_file, meta)

	def __get_file_by_key(self, key):
		"""
		Calculates the corresponding json file for 'key'
		"""

		file  = self.__root_dir
		chars = key[:(self.__max_depth - 1)]

		for c in chars:
			file = os.path.join(file, c)

		file = os.path.join(
			file,
			key[(self.__max_depth - 1):self.__max_depth] + '.json'
		)

		return file

	def __load_dict_from_json_file(self, path_to_file):
		"""
		Returns the content of the json file 'path_to_file' as dict or an empty
		dict, when something went wrong. If something went wrong, maintain_kvs()
		gets called (the meta file is an exception here).
		"""

		try:
			with open(path_to_file, 'r', encoding='UTF-8') as f:
				data_as_dict = json.load(f)
				f.close()
			return data_as_dict
		except:
			self.__save_dict_to_json_file(path_to_file, {})
			if not path_to_file == self.__meta_file:
				self.maintain_kvs()

		return {}

	def __load_meta_file(self):
		"""
		Reads the meta file or rebuilds it, if it is unreadable (broken/lost)
		"""

		def load_max_depth(meta):
			if not 'max_depth' in meta:
				return False
			if not isinstance(meta['max_depth'], int):
				return False
			if not meta['max_depth'] in range(1, 7):
				return False
			self.__max_depth = meta['max_depth']
			return True

		def load_entries(meta):
			if not 'entries' in meta:
				return False
			if not isinstance(meta['max_depth'], int):
				return False
			if not meta['entries'] >= 0:
				return False
			self.__entries = meta['entries']
			return True

		meta = self.__load_dict_from_json_file(self.__meta_file)

		if meta == {}:
			meta_file_restored = self.__restore_meta_file()
			if not meta_file_restored:
				return False
			self.maintain_kvs()
			meta = self.__load_dict_from_json_file(self.__meta_file)

		if not load_max_depth(meta):
			return False
		if not load_entries(meta):
			return False

		return True

	def __process_key(self, key):
		"""
		Processes the key 'key' to match the filesystem based hash table
		"""

		if len(key) < self.__max_depth:
			return self.__str_to_sha256sum(key)

		char_whitelist = [c for c in '0123456789abcdef']
		for c in key:
			if c not in char_whitelist:
				return self.__str_to_sha256sum(key)

		return key

	def __restore_meta_file(self):
		"""
		Tries to restore an broken or lost meta file by guessing the 'max_depth'
		"""

		def get_max_depth(r_root_dir, r_current_depth=1):
			# build all file paths recursivly
			r_chars = [c for c in '0123456789abcdef']
			# base case 1: r_current_depth > 7
			if r_current_depth > 7:
				return -1
			# base case 2: json file found
			for c in r_chars:
				file = os.path.join(r_root_dir, c + '.json')
				if os.path.exists(file):
					return r_current_depth
			# recursion: go one step further in the filesystem
			for c in r_chars:
				max_depth_guess = get_max_depth(
					os.path.join(r_root_dir, c),
					r_current_depth + 1
				)
				if max_depth_guess > -1:
					return max_depth_guess
			return -1

		max_depth = get_max_depth(self.__root_dir)

		if max_depth > -1:
			self.__max_depth = max_depth
			return self.__create_meta_file()

		return False

	def __save_dict_to_json_file(self, path_to_file, data_as_dict):
		"""
		Tries to write the dict 'data_as_dict' to the json file 'path_to_file'
		"""

		try:
			with open(path_to_file, 'w', encoding='UTF-8') as f:
				f.write(
					json.dumps(
						data_as_dict,
						ensure_ascii=False
					)
				)
				f.close()
			return True
		except:
			return False

	def __str_to_sha256sum(self, s):
		"""
		Returns a hexdigit sha256sum as string for the sring 's'
		"""

		return hashlib.sha256(bytes(s, 'utf-8')).hexdigest()

	def __validate_key(self, key):
		"""
		Validates, if the key 'key' can be used for the kvs
		"""

		if not isinstance(key, str):
			raise ValueError("key must be of type <class 'str'>")
		if key == '':
			raise ValueError("key must not be empty")
		if len(key) > 64:
			raise ValueError("key max length is 64 characters")

	def __validate_value(self, value):
		"""
		Validates, if the value 'value' can be used for the kvs
		"""

		if isinstance(value, list):
			for v in value:
				self.__validate_value(v)
			return

		if isinstance(value, dict):
			for k, v in value.items():
				self.__validate_key(k)
				self.__validate_value(v)
			return

		if not isinstance(value, (str, int, float, bool)):
			raise ValueError(
				"value must be of type"
				+ " <class 'str'>, <class 'int'>, <class 'float'>"
				+ " or <class 'bool'>"
			)
