import filecmp
import os
import shutil
import tempfile
from unittest import TestCase

from edgar import parse_line_to_record, parse_html, parse_mda, combine_indices_to_csv


class TestEdgar(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_dir)

    def test_parse_line_to_record(self):
        line = "10-K        1347 Capital Corp                                             1606163     2016-03-21  edgar/data/1606163/0001144204-16-089184.txt"
        fields_begin = [0, 12, 74, 86, 98]
        expected_output = ["10-K", "1347 Capital Corp", "1606163",
                           "2016-03-21", "edgar/data/1606163/0001144204-16-089184.txt"]
        output = parse_line_to_record(line, fields_begin)
        self.assertEqual(output, expected_output)

    def test_parse_html(self):
        input_file = "test_data/example.form10k.txt"
        output_file = os.path.join(self.test_dir, "parsed_html.txt")
        parse_html(input_file, output_file)
        ref_file = "test_data/example.form10k.parsed.txt"
        self.assertTrue(filecmp.cmp(output_file, ref_file))

    def test_parse_mda(self):
        input_file = "test_data/example.form10k.parsed.txt"
        output_file = os.path.join(self.test_dir, "mda.txt")
        parse_mda(input_file, output_file)
        ref_file = "test_data/example.mda.txt"
        self.assertTrue(filecmp.cmp(output_file, ref_file))
