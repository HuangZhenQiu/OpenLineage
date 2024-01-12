#!/bin/bash
#
# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# NOTE: This script was inspired by https://github.com/MarquezProject/marquez/blob/main/new-version.sh
#
# Requirements:
#   * You're on the 'main' branch
#   * You've installed 'bump2version'
#
# Usage: $ ./new-version.sh --release-version RELEASE_VERSION --next-version NEXT_VERSION

set -e

title() {
  echo -e "\033[1m${1}\033[0m"
}

usage() {
  echo "Usage: ./$(basename -- "${0}") --release-version RELEASE_VERSION --next-version NEXT_VERSION"
  echo "A script used to release OpenLineage"
  echo
  title "EXAMPLES:"
  echo "  # Bump version ('-SNAPSHOT' will automatically be appended to '0.0.2')"
  echo "  $ ./new-version.sh -r 0.0.1 -n 0.0.2"
  echo
  echo "  # Bump version (with '-SNAPSHOT' already appended to '0.0.2')"
  echo "  $ ./new-version.sh -r 0.0.1 -n 0.0.2-SNAPSHOT"
  echo
  echo "  # Bump release candidate"
  echo "  $ ./new-version.sh -r 0.0.1-rc.1 -n 0.0.2-rc.2"
  echo
  echo "  # Bump release candidate without push"
  echo "  $ ./new-version.sh -r 0.0.1-rc.1 -n 0.0.2-rc.2 -p"
  echo
  title "ARGUMENTS:"
  echo "  -r, --release-version string     the release version (ex: X.Y.Z, X.Y.Z-rc.*)"
  echo "  -n, --next-version string        the next version (ex: X.Y.Z, X.Y.Z-SNAPSHOT)"
  echo
  title "FLAGS:"
  echo "  -p, --no-push     local changes are not automatically pushed to the remote repository"
  exit 1
}

# Update the python package version only if the current_version is different from the new_version
# We do this check because bumpversion screws up the search/replace if the current_version and
# new_version are the same
function update_py_version_if_needed() {
  export $(bump2version manual --new-version $1 --allow-dirty --list --dry-run | grep version | xargs)
  if [ "$new_version" != "$current_version" ]; then
    bump2version manual --new-version $1 --allow-dirty
  fi
}

readonly SEMVER_REGEX="^[0-9]+(\.[0-9]+){2}((\-[0-9]+)?(-SNAPSHOT)?)$" # X.Y.Z
                                                                          # X.Y.Z-rc.*
                                                                          # X.Y.Z-rc.*-SNAPSHOT
                                                                          # X.Y.Z-SNAPSHOT

# Change working directory to project root
project_root=$(git rev-parse --show-toplevel)
cd "${project_root}/"

# Verify bump2version is installed
# if [[ ! $(type -P bump2version) ]]; then
#  echo "bump2version not installed! Please see https://github.com/c4urself/bump2version#installation"
#  pip install --upgrade bump2version
#  echo "bump2version is on demand installed!"
# fi

# if [[ $# -eq 0 ]] ; then
#  usage
# fi

# Ensure no unstaged changes are present in working directory
if [[ -n "$(git status --porcelain --untracked-files=no)" ]] ; then
  echo "error: you have unstaged changes in your working directory!"
  exit 1;
fi

# Ensure valid versions
VERSIONS=(${BUILD_PARAM_RELEASE_VERSION})
for VERSION in "${VERSIONS[@]}"; do
  if [[ ! "${VERSION}" =~ ${SEMVER_REGEX} ]]; then
    echo "Error: Version '${VERSION}' must match '${SEMVER_REGEX}'"
    exit 1
  fi
done

RELEASE_VERSION=${BUILD_PARAM_RELEASE_VERSION}"-ase-apple"
echo "release version is: " $RELEASE_VERSION

# Ensure python module version matches X.Y.Z or X.Y.ZrcN (see: https://www.python.org/dev/peps/pep-0440/),
PYTHON_RELEASE_VERSION=${RELEASE_VERSION}
if [[ "${RELEASE_VERSION}" == *-rc.? ]]; then
  RELEASE_CANDIDATE=${RELEASE_VERSION##*-}
  PYTHON_RELEASE_VERSION="${RELEASE_VERSION%-*}${RELEASE_CANDIDATE//.}"
fi

# (1) Bump python module versions. Do this before the release in case the current release is not
# the same version as what was expected the last time we released. E.g., if the next expected
# release was a patch version, but a new minor version is being released, we need to update to the
# actual release version prior to committing/tagging
# PYTHON_MODULES=(client/python/ integration/common/ integration/airflow/ integration/dbt/ integration/dagster/ integration/sql)
# for PYTHON_MODULE in "${PYTHON_MODULES[@]}"; do
#  (cd "${PYTHON_MODULE}" && update_py_version_if_needed "${PYTHON_RELEASE_VERSION}")
# done

# (2) Bump java module versions
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./client/java/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/sql/iface-java/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/spark/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/flink/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./integration/flink/examples/stateful/gradle.properties
perl -i -pe"s/^version=.*/version=${RELEASE_VERSION}/g" ./proxy/backend/gradle.properties

# (4) Prepare release commit
git commit -sam "Prepare for release ${RELEASE_VERSION}"

echo "DONE!"
