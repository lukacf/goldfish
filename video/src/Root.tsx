import React from 'react';
import {Composition} from 'remotion';

import {GoldfishIntroVideo} from './components/GoldfishIntroVideo';

export const RemotionRoot: React.FC = () => {
  return (
    <>
      <Composition
        id="GoldfishIntro"
        component={GoldfishIntroVideo}
        durationInFrames={1800}
        fps={30}
        width={1920}
        height={1080}
        defaultProps={{variant: 'default'}}
      />
    </>
  );
};
