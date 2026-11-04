"""Package identity survives archives and distinguishes copied/modified sources."""
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)) if str(ROOT) not in sys.path else None
from custom_components.eybond_local import build_identity as identity
from custom_components.eybond_local.support.masking import mask_numeric_identifiers


class BuildIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'component'
        self.root.mkdir()
        (self.root/'manifest.json').write_text('{"version":"0.3.0-beta.5"}')
        (self.root/'__init__.py').write_text('VALUE = 1\n')

    def read(self):
        return identity.read_build_runtime_values(self.root)

    def test_raw_copy_has_source_id_without_invented_commit(self):
        values = self.read()
        self.assertRegex(values['integration_build_id'], r'^sha256:[a-f0-9]{64}$')
        self.assertNotIn('integration_build_git_commit', values)
        self.assertEqual(values['integration_build_metadata_status'], 'commit_unavailable')

    def test_checkout_placeholders_are_not_commit_or_date(self):
        shutil.copyfile(ROOT/'custom_components/eybond_local/BUILD_INFO.txt', self.root/'BUILD_INFO.txt')
        values = self.read()
        self.assertTrue(values['integration_build_info_present'])
        self.assertNotIn('integration_build_git_commit', values)
        self.assertNotIn('integration_build_commit_date', values)
        self.assertNotIn('$Format', identity.build_label(values))

    def test_legacy_stamp_is_a_declaration_not_a_clean_flag(self):
        (self.root/'BUILD_INFO.txt').write_text('Git commit: ' + 'abcdef01' * 5 + '\n')
        before = self.read()
        (self.root/'__init__.py').write_text('VALUE = 2\n')
        after = self.read()
        self.assertEqual(before['integration_build_git_commit'], after['integration_build_git_commit'])
        self.assertNotEqual(before['integration_build_id'], after['integration_build_id'])
        self.assertNotIn('clean', repr(after))

    def test_build_metadata_and_bytecode_do_not_change_source_id(self):
        before = self.read()
        (self.root/'BUILD_INFO.txt').write_text('Git commit: abcdef0\n')
        (self.root/'__pycache__').mkdir()
        (self.root/'__pycache__/cached.pyc').write_bytes(b'cache')
        self.assertEqual(before['integration_build_id'], self.read()['integration_build_id'])

    def test_profile_translation_and_path_are_part_of_fingerprint(self):
        before = self.read()['integration_build_id']
        (self.root/'profile.json').write_text('{}')
        after = self.read()['integration_build_id']
        (self.root/'profile.json').rename(self.root/'translation.json')
        self.assertNotEqual(before, after)
        self.assertNotEqual(after, self.read()['integration_build_id'])

    def test_same_sources_in_other_directory_have_same_fingerprint(self):
        destination = Path(self.temp.name)/'copied'
        shutil.copytree(self.root, destination)
        self.assertEqual(self.read()['integration_build_id'],
                         identity.read_build_runtime_values(destination)['integration_build_id'])

    def test_snapshot_does_not_relabel_running_process_after_disk_update(self):
        before = identity.build_runtime_values(self.root)
        (self.root/'__init__.py').write_text('VALUE = 42\n')
        self.assertEqual(before, identity.build_runtime_values(self.root))
        self.assertNotEqual(before['integration_build_id'], self.read()['integration_build_id'])
        before['integration_build_id'] = 'tampered caller copy'
        self.assertNotEqual(before, identity.build_runtime_values(self.root))

    def test_bad_metadata_does_not_break_fingerprint_or_expose_fields(self):
        for raw in ('git_commit: account-secret\npassword: private\n', 'x' * 16385):
            with self.subTest(raw_length=len(raw)):
                (self.root/'BUILD_INFO.txt').write_text(raw)
                values = self.read()
                self.assertNotIn('integration_build_git_commit', values)
                self.assertEqual(values['integration_build_fingerprint_status'], 'available')
                self.assertNotIn('private', repr(values))

    def test_symlink_source_is_not_read_or_given_false_identity(self):
        (self.root/'outside.py').symlink_to(Path(self.temp.name)/'not-readable')
        self.assertEqual(self.read()['integration_build_fingerprint_status'], 'unavailable')

    def test_size_and_count_limits_degrade_only_diagnostics(self):
        for name, value in (('_MAX_FILES', 1), ('_MAX_FILE_BYTES', 1), ('_MAX_BYTES', 1)):
            with self.subTest(limit=name), patch.object(identity, name, value):
                self.assertEqual(self.read()['integration_build_id'], 'unknown')
                self.assertEqual(self.read()['integration_manifest_version'], '0.3.0-beta.5')

    def test_missing_package_is_unknown_not_a_hash_of_nothing(self):
        values = identity.read_build_runtime_values(self.root/'missing')
        self.assertEqual(values['integration_build_id'], 'unknown')
        self.assertEqual(values['integration_manifest_version'], '')

    def test_support_masking_preserves_only_well_formed_typed_identity(self):
        digest = 'a' + '1234567890' * 6 + 'abc'
        values = {'integration_build_id':'sha256:'+digest, 'integration_build_fingerprint':digest,
                  'integration_build_git_commit':digest[:40], 'arbitrary_hash':digest,
                  'serial':'92632500000001'}
        masked = mask_numeric_identifiers(values)
        for key in ('integration_build_id','integration_build_fingerprint','integration_build_git_commit'):
            self.assertEqual(values[key], masked[key])
        self.assertIn('*', masked['serial'])
        self.assertIn('*', masked['arbitrary_hash'])
        self.assertIn('*', mask_numeric_identifiers({'integration_build_id':'PN=92632500000001'})['integration_build_id'])
        self.assertIn('*', mask_numeric_identifiers({'integration_build_git_commit':'92632500000001'})['integration_build_git_commit'])

    def test_support_zip_keeps_full_identity_through_all_sanitizers(self):
        from custom_components.eybond_local.support.package import export_support_package
        digest = 'a' + '1234567890' * 6 + 'abc'
        values = {'integration_build_id':'sha256:'+digest, 'integration_build_fingerprint':digest,
                  'integration_build_git_commit':digest[:40], 'serial':'92632500000001'}
        result = export_support_package(config_dir=Path(self.temp.name), entry_id='synthetic',
            entry_title='Synthetic', support_bundle={'runtime':{'metadata':values}},
            raw_capture=None, fixture=None, anonymized_fixture=None)
        with zipfile.ZipFile(result.path) as archive:
            bundled = json.loads(archive.read('support_bundle.json'))
        actual = bundled['runtime']['metadata']
        for key in ('integration_build_id','integration_build_fingerprint','integration_build_git_commit'):
            self.assertEqual(actual[key], values[key])
        self.assertNotIn(values['serial'], repr(bundled))

    def test_git_archive_stamps_commit_and_hacs_component_copy_keeps_it(self):
        repo = Path(self.temp.name)/'repo'; repo.mkdir()
        package = repo/'custom_components/eybond_local'
        package.parent.mkdir(); shutil.copytree(self.root, package)
        shutil.copyfile(ROOT/'.gitattributes', repo/'.gitattributes')
        shutil.copyfile(ROOT/'custom_components/eybond_local/BUILD_INFO.txt', package/'BUILD_INFO.txt')
        def git(*args):
            return subprocess.check_output(['git','-c','commit.gpgsign=false','-c','core.hooksPath=/dev/null',
                '-c','user.name=Build Test','-c','user.email=build@example.invalid',*args], cwd=repo, stderr=subprocess.DEVNULL)
        git('init','-q'); git('add','.'); git('commit','-qm','fixture')
        commit = git('rev-parse','HEAD').decode().strip()
        archive = zipfile.ZipFile(io.BytesIO(git('archive','--format=zip','--prefix=repository/', 'HEAD')))
        installed = Path(self.temp.name)/'installed'; installed.mkdir()
        prefix = 'repository/custom_components/eybond_local/'
        for info in archive.infolist():
            if info.filename.startswith(prefix) and not info.is_dir():
                target = installed/info.filename.removeprefix(prefix)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.read(info))
        values = identity.read_build_runtime_values(installed)
        self.assertEqual(values['integration_build_git_commit'], commit)
        self.assertEqual(values['integration_build_fingerprint'],
                         identity.read_build_runtime_values(package)['integration_build_fingerprint'])
        self.assertNotIn('$Format', (installed/'BUILD_INFO.txt').read_text())


if __name__ == '__main__':
    unittest.main()
