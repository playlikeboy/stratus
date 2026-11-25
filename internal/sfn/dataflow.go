package sfn

// The data-flow helpers of a state: InputPath, ResultPath and OutputPath.
// None of them modifies its arguments.

func applyInputPath(o optRef, input any, e *env) (any, error) {
	return applyOptRef(o, "InputPath", input, e)
}

func applyOutputPath(o optRef, value any, e *env) (any, error) {
	return applyOptRef(o, "OutputPath", value, e)
}

func applyOptRef(o optRef, what string, value any, e *env) (any, error) {
	switch {
	case !o.set:
		return value, nil
	case o.null:
		return map[string]any{}, nil
	}
	v, ok := o.r.eval(&env{input: value, ctx: e.ctx})
	if !ok {
		return nil, runtimeError("the %s %s could not be found in the state's data", what, o.r.p.raw)
	}
	return v, nil
}

// applyResultPath merges result into the state's raw input.
func applyResultPath(rp resultPath, rawInput, result any) any {
	switch {
	case !rp.set:
		return result
	case rp.null:
		return rawInput
	}
	return rp.p.set(rawInput, result)
}

// finishData runs the tail of the pipeline shared by Pass and Task:
// ResultSelector, ResultPath and OutputPath.
func finishData(st *state, rawInput, result any, e *env) (any, error) {
	if st.resultSelector != nil {
		r, err := st.resultSelector.eval(&env{input: result, ctx: e.ctx})
		if err != nil {
			return nil, err
		}
		result = r
	}
	merged := applyResultPath(st.resultPath, rawInput, result)
	return applyOutputPath(st.outputPath, merged, e)
}
