#!/usr/bin/env python3
"""Exercise the real interactive wrapper with a mocked engine and controlling tty."""
import os
from pathlib import Path
import pty
import select
import shutil
import tempfile
import time
import unittest


class InstallWrapperTest(unittest.TestCase):
    def run_wrapper(self, args=(), answers="", config="", complete=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = Path(__file__).resolve().parent
            shutil.copy(scripts / "install.sh", root)
            shutil.copy(scripts / "common.sh", root)
            with (root / "common.sh").open("a") as common:
                common.write("\nrequire_root_linux_amd64() { :; }\n"
                             "fetch_latest_release() { RELEASE_TAG=v-test; }\n"
                             "detect_advertise_ipv4() { echo 192.0.2.10; }\n")
            (root / "install-core.sh").write_text(
                '#!/bin/bash\nprintf "%s\\n" "$@" >"$CALL_LOG"\n'
                'echo "confirmed=${TRELLIS_INSTALL_PLAN_CONFIRMED:-0}" >>"$CALL_LOG"\n')
            (root / "bin").mkdir()
            if config:
                (root / "config").write_text(config)
            if complete:
                (root / "state").write_text("complete=true\n")
                (root / "bin/trellis").write_text("#!/bin/sh\n")
                (root / "bin/trellis").chmod(0o755)
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(("TRELLIS_", "STATE_", "CONFIG_", "GVISOR_"))}
            env.update(INSTALL_DIR=str(root / "bin"), STATE_FILE=str(root / "state"),
                       CONFIG_FILE=str(root / "config"), SERVICE_FILE=str(root / "service"),
                       CALL_LOG=str(root / "calls"), NO_COLOR="1")
            pid, fd = pty.fork()
            if pid == 0:
                os.execvpe("bash", ["bash", str(root / "install.sh"), *args], env)
            output = bytearray()
            deadline = time.monotonic() + 10
            try:
                os.write(fd, answers.encode())
                while time.monotonic() < deadline:
                    if select.select([fd], [], [], 0.1)[0]:
                        try:
                            block = os.read(fd, 65536)
                        except OSError:
                            break
                        if not block:
                            break
                        output.extend(block)
                else:
                    os.kill(pid, 9)
                    self.fail("wrapper timed out: " + output.decode())
            finally:
                os.close(fd)
                _, status = os.waitpid(pid, 0)
            log = (root / "calls").read_text() if (root / "calls").exists() else ""
            return os.waitstatus_to_exitcode(status), output.decode(), log

    def test_default_confirmation(self):
        rc, output, log = self.run_wrapper(answers="\n")
        self.assertEqual(rc, 0, output)
        self.assertIn("Version       v-test", output)
        self.assertIn("--advertise\n192.0.2.10\n", log)
        self.assertIn("--with-gvisor\nconfirmed=1", log)

    def test_cancel(self):
        rc, output, log = self.run_wrapper(answers="q\n")
        self.assertEqual(rc, 0, output)
        self.assertIn("No changes made", output)
        self.assertEqual(log, "")

    def test_customize(self):
        rc, output, log = self.run_wrapper(
            answers="c\n1\n2\ncontrol:8128\n2\n192.0.2.42\n3\n4\n5\n\n\n")
        self.assertEqual(rc, 0, output)
        self.assertIn("--join\ncontrol:8128\n", log)
        self.assertIn("--advertise\n192.0.2.42\n", log)
        self.assertIn("--control-plane\nfalse\n--runs-workloads\nfalse\n", log)
        self.assertNotIn("--with-gvisor", log)

    def test_resume_fixed_settings(self):
        rc, output, log = self.run_wrapper(
            answers="c\n1\n2\n4\n5\n\n\n",
            config="agent_advertise: original:8127\njoin: control:8128\ncontrol_plane: false\nruns_workloads: false\n")
        self.assertEqual(rc, 0, output)
        self.assertIn("fixed while resuming", output)
        self.assertIn("--advertise\noriginal\n", log)
        self.assertIn("--control-plane\nfalse\n--runs-workloads\nfalse\n", log)

    def test_automation_forwards_inputs(self):
        args = ["--yes", "--worker", "--join", "control:8128", "--without-gvisor",
                "--join-token-file", "/token", "--ca-cert-file", "/ca",
                "--secrets-key-file", "/key", "--secrets-key-id", "key-id"]
        rc, output, log = self.run_wrapper(args)
        self.assertEqual(rc, 0, output)
        for flag, value in zip(args[5::2], args[6::2]):
            self.assertIn(flag + "\n" + value + "\n", log)
        self.assertIn("confirmed=0", log)

    def test_completed_fast_path(self):
        rc, output, log = self.run_wrapper(config="control_plane: true\n", complete=True)
        self.assertEqual(rc, 0, output)
        self.assertEqual(log, "--yes\nconfirmed=0\n")

    def test_invalid_role(self):
        rc, output, log = self.run_wrapper(["--control-plane", "maybe"])
        self.assertNotEqual(rc, 0)
        self.assertIn("requires true or false", output)
        self.assertEqual(log, "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
