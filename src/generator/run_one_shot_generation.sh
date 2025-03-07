#!/bin/bash

# synthesis from jsons
bash src/generator/generate_program.sh flights
bash src/generator/generate_program.sh rents
bash src/generator/generate_program.sh hospital

# direct synthesis
# python src/generator/generate_program_claude_direct_synthesis.py