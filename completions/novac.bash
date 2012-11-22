_novac() {
  COMPREPLY=()
  local word="${COMP_WORDS[COMP_CWORD]}"

  if [ "$COMP_CWORD" -eq 1 ]; then
    COMPREPLY=( $(compgen -W "$(novac commands)" -- "$word") )
  else
    local command="${COMP_WORDS[1]}"
    local completions="$(novac completions "$command" ${COMP_WORDS[@]:2})"
    COMPREPLY=( $(compgen -W "$completions" -- "$word") )
  fi
}

complete -F _novac novac
